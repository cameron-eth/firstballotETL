"""
Audit auth.users vs user_profiles table.
Lists all auth users and ensures each has a corresponding user_profile record.
"""
import os
import uuid
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from supabase import create_client


def get_client():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    return create_client(url, key)


def list_all_auth_users(client):
    """Fetch every user from auth.users using the admin API (paginates automatically)."""
    all_users = []
    page = 1
    per_page = 100
    while True:
        resp = client.auth.admin.list_users(page=page, per_page=per_page)
        # supabase-py v2 returns a list directly
        users = resp if isinstance(resp, list) else getattr(resp, "users", resp)
        if not users:
            break
        all_users.extend(users)
        if len(users) < per_page:
            break
        page += 1
    return all_users


def list_all_profiles(client):
    """Fetch all user_profiles rows."""
    rows = []
    offset = 0
    limit = 1000
    while True:
        resp = (
            client.table("user_profiles")
            .select("id, auth_id, email, username, membership_status, feature_access")
            .range(offset, offset + limit - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return rows


def main():
    client = get_client()

    # --- 1. Pull auth users ---
    print("=" * 70)
    print("PULLING AUTH USERS")
    print("=" * 70)
    auth_users = list_all_auth_users(client)
    print(f"Total auth.users records: {len(auth_users)}")
    for u in auth_users:
        uid = u.id if hasattr(u, "id") else u.get("id")
        email = u.email if hasattr(u, "email") else u.get("email")
        meta = u.user_metadata if hasattr(u, "user_metadata") else u.get("user_metadata", {})
        username = meta.get("username", "") if meta else ""
        created = u.created_at if hasattr(u, "created_at") else u.get("created_at", "")
        print(f"  auth_id={uid}  email={email}  username={username}  created={created}")

    # --- 2. Pull user_profiles ---
    print()
    print("=" * 70)
    print("PULLING USER PROFILES")
    print("=" * 70)
    profiles = list_all_profiles(client)
    print(f"Total user_profiles records: {len(profiles)}")
    profile_auth_ids = set()
    for p in profiles:
        profile_auth_ids.add(p["auth_id"])
        print(f"  profile_id={p['id']}  auth_id={p['auth_id']}  email={p['email']}  "
              f"username={p['username']}  member={p.get('membership_status')}  "
              f"feature_access={p.get('feature_access')}")

    # --- 3. Find auth users without profiles ---
    print()
    print("=" * 70)
    print("AUDIT RESULTS")
    print("=" * 70)
    missing = []
    for u in auth_users:
        uid = u.id if hasattr(u, "id") else u.get("id")
        if uid not in profile_auth_ids:
            missing.append(u)

    if not missing:
        print("✅ Every auth.users record has a matching user_profiles row.")
    else:
        print(f"⚠️  {len(missing)} auth user(s) WITHOUT a user_profile:")
        for u in missing:
            uid = u.id if hasattr(u, "id") else u.get("id")
            email = u.email if hasattr(u, "email") else u.get("email")
            print(f"  MISSING: auth_id={uid}  email={email}")

    # --- 4. Create missing profiles ---
    if missing:
        print()
        print("=" * 70)
        print("CREATING MISSING PROFILES")
        print("=" * 70)
        for u in missing:
            uid = u.id if hasattr(u, "id") else u.get("id")
            email = u.email if hasattr(u, "email") else u.get("email", "unknown")
            meta = u.user_metadata if hasattr(u, "user_metadata") else u.get("user_metadata", {})
            username = (meta or {}).get("username", "")
            if not username:
                username = email.split("@")[0] if email else "user"

            profile_data = {
                "id": str(uuid.uuid4()),
                "auth_id": uid,
                "email": email,
                "username": username,
                "sleeper_username": None,
                "favorite_team": None,
                "sleeper_league_id": None,
                "membership_status": False,
                "feature_access": False,
                "stripe_id": None,
            }

            try:
                resp = (
                    client.table("user_profiles")
                    .insert(profile_data)
                    .execute()
                )
                print(f"  ✅ Created profile for {email} (auth_id={uid})")
            except Exception as e:
                print(f"  ❌ Failed to create profile for {email}: {e}")

    # --- 5. Also check for orphaned profiles (profile without matching auth user) ---
    auth_ids = set()
    for u in auth_users:
        uid = u.id if hasattr(u, "id") else u.get("id")
        auth_ids.add(uid)

    orphaned = [p for p in profiles if p["auth_id"] not in auth_ids]
    if orphaned:
        print()
        print("=" * 70)
        print("⚠️  ORPHANED PROFILES (profile exists but no auth user)")
        print("=" * 70)
        for p in orphaned:
            print(f"  ORPHAN: profile_id={p['id']}  auth_id={p['auth_id']}  email={p['email']}")
    else:
        print("\n✅ No orphaned profiles found.")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
