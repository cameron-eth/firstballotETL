-- Fix auth.users -> public.user_profiles consistency at the source.
-- 1) Backfill missing profile rows for existing auth users.
-- 2) Add a trigger so all future auth users always get a profile row.

begin;

-- Backfill: create a profile for any auth user without one.
insert into public.user_profiles (
  auth_id,
  email,
  username,
  sleeper_username,
  favorite_team,
  sleeper_league_id,
  membership_status,
  feature_access,
  stripe_id
)
select
  au.id as auth_id,
  au.email,
  coalesce(
    nullif(trim(au.raw_user_meta_data ->> 'username'), ''),
    split_part(au.email, '@', 1),
    'user'
  ) as username,
  null as sleeper_username,
  null as favorite_team,
  null as sleeper_league_id,
  false as membership_status,
  false as feature_access,
  null as stripe_id
from auth.users au
where au.deleted_at is null
  and not exists (
    select 1
    from public.user_profiles up
    where up.auth_id = au.id
  );

-- Trigger function: create profile row whenever a new auth user is inserted.
create or replace function public.handle_new_auth_user_profile()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.deleted_at is null then
    insert into public.user_profiles (
      auth_id,
      email,
      username,
      sleeper_username,
      favorite_team,
      sleeper_league_id,
      membership_status,
      feature_access,
      stripe_id
    )
    select
      new.id,
      new.email,
      coalesce(
        nullif(trim(new.raw_user_meta_data ->> 'username'), ''),
        split_part(new.email, '@', 1),
        'user'
      ),
      null,
      null,
      null,
      false,
      false,
      null
    where not exists (
      select 1
      from public.user_profiles up
      where up.auth_id = new.id
    );
  end if;

  return new;
end;
$$;

drop trigger if exists on_auth_user_created_create_profile on auth.users;

create trigger on_auth_user_created_create_profile
after insert on auth.users
for each row execute function public.handle_new_auth_user_profile();

commit;
