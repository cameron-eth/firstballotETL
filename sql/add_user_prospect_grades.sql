-- Run in Supabase SQL Editor (once). Safe to re-run.
-- Per-user film/talent grades for the Scouting Portal big board.
-- Private to each user via RLS; shaped so a community average can be added later.
--
-- user_id holds auth.users.id (the x-user-id header value), matching
-- user_draftboards.user_id -- NOT user_profiles.id, which is a separate
-- app-generated UUID.

CREATE TABLE IF NOT EXISTS user_prospect_grades (
  user_id      UUID    NOT NULL REFERENCES auth.users(id)        ON DELETE CASCADE,
  prospect_id  INTEGER NOT NULL REFERENCES dynasty_prospects(id) ON DELETE CASCADE,
  film_grade   NUMERIC(5,2) CHECK (film_grade   >= 0 AND film_grade   <= 100),
  talent_grade NUMERIC(5,2) CHECK (talent_grade >= 0 AND talent_grade <= 100),
  -- MY GRADE: 50/50 when both are set, otherwise whichever one exists.
  -- Mirrored in firstballot/lib/user-grade.ts for optimistic UI updates.
  -- Change both together.
  my_grade     NUMERIC(5,2) GENERATED ALWAYS AS (
    CASE
      WHEN film_grade IS NOT NULL AND talent_grade IS NOT NULL
        THEN (film_grade + talent_grade) / 2
      ELSE COALESCE(film_grade, talent_grade)
    END
  ) STORED,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, prospect_id),
  -- A row with neither grade is meaningless; the API deletes instead.
  CONSTRAINT user_prospect_grades_not_empty
    CHECK (film_grade IS NOT NULL OR talent_grade IS NOT NULL)
);

-- Supports the future "average grade for this prospect across users" query.
-- The primary key already covers the per-user lookup the board does on load.
CREATE INDEX IF NOT EXISTS idx_user_prospect_grades_prospect
  ON user_prospect_grades(prospect_id);

CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS user_prospect_grades_set_updated_at ON user_prospect_grades;
CREATE TRIGGER user_prospect_grades_set_updated_at
  BEFORE UPDATE ON user_prospect_grades
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- The app reaches this table through createAuthenticatedSupabaseClient(), which
-- relies on RLS for per-user isolation. Without these policies every logged-in
-- user could read every other user's grades.
ALTER TABLE user_prospect_grades ENABLE ROW LEVEL SECURITY;

-- CREATE POLICY has no IF NOT EXISTS; drop-then-create keeps this re-runnable.
DROP POLICY IF EXISTS user_prospect_grades_select_own ON user_prospect_grades;
CREATE POLICY user_prospect_grades_select_own ON user_prospect_grades
  FOR SELECT USING (auth.uid() = user_id);

DROP POLICY IF EXISTS user_prospect_grades_insert_own ON user_prospect_grades;
CREATE POLICY user_prospect_grades_insert_own ON user_prospect_grades
  FOR INSERT WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS user_prospect_grades_update_own ON user_prospect_grades;
CREATE POLICY user_prospect_grades_update_own ON user_prospect_grades
  FOR UPDATE USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS user_prospect_grades_delete_own ON user_prospect_grades;
CREATE POLICY user_prospect_grades_delete_own ON user_prospect_grades
  FOR DELETE USING (auth.uid() = user_id);
