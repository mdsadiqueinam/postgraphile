-- ============================================================
-- Test seed data for turbograph introspection & query tests
-- ============================================================

-- -------------------------------------------------------
-- users
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.users (
    id         SERIAL       PRIMARY KEY,
    username   VARCHAR(50)  NOT NULL UNIQUE,
    email      VARCHAR(255) NOT NULL UNIQUE,
    bio        TEXT,
    is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  public.users             IS 'Registered users of the application.';
COMMENT ON COLUMN public.users.bio        IS 'Optional short biography.';

-- -------------------------------------------------------
-- posts
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.posts (
    id           SERIAL       PRIMARY KEY,
    author_id    INT          NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    title        VARCHAR(255) NOT NULL,
    body         TEXT         NOT NULL,
    is_published BOOLEAN      NOT NULL DEFAULT FALSE,
    views        INT          NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.posts IS 'Blog posts written by users.';

-- -------------------------------------------------------
-- comments
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.comments (
    id         SERIAL      PRIMARY KEY,
    post_id    INT         NOT NULL REFERENCES public.posts(id)  ON DELETE CASCADE,
    author_id  INT         NOT NULL REFERENCES public.users(id)  ON DELETE CASCADE,
    body       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE public.comments IS 'Comments left on posts.';

-- -------------------------------------------------------
-- tags
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.tags (
    id   SERIAL      PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

COMMENT ON TABLE public.tags IS 'Categorisation tags.';

-- -------------------------------------------------------
-- post_tags  (many-to-many junction)
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.post_tags (
    post_id INT NOT NULL REFERENCES public.posts(id) ON DELETE CASCADE,
    tag_id  INT NOT NULL REFERENCES public.tags(id)  ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

COMMENT ON TABLE public.post_tags IS 'Associates posts with tags. @omit create,update,delete';

-- ==============================================================
-- Seed data
-- ==============================================================

INSERT INTO public.users (username, email, bio, is_active) VALUES
    ('alice',   'alice@example.com',   'Full-stack developer and coffee enthusiast.',  TRUE),
    ('bob',     'bob@example.com',     'Backend engineer who loves Rust.',              TRUE),
    ('charlie', 'charlie@example.com', 'Designer turned developer.',                   TRUE),
    ('diana',   'diana@example.com',   NULL,                                            TRUE),
    ('eve',     'eve@example.com',     'Security researcher.',                          FALSE);

INSERT INTO public.posts (author_id, title, body, is_published, views) VALUES
    (1, 'Getting Started with Rust',        'Rust is a systems programming language focused on safety...', TRUE,  320),
    (1, 'Understanding Ownership',          'Ownership is Rust''s most unique feature...',                  TRUE,  185),
    (2, 'PostgreSQL Performance Tips',      'Index usage is critical for query performance...',             TRUE,  540),
    (2, 'Draft: Async Rust Deep Dive',      'This post is still in progress.',                              FALSE,   0),
    (3, 'GraphQL vs REST',                  'Both GraphQL and REST have their place...',                    TRUE,  210),
    (3, 'Designing Good APIs',              'Good API design starts with the consumer...',                  TRUE,   95),
    (4, 'My First Post',                    'Hello world! This is my first blog post.',                     TRUE,   12),
    (1, 'Turbograph from Scratch',        'Turbograph auto-generates a GraphQL schema from Postgres.', TRUE,  430);

INSERT INTO public.tags (name) VALUES
    ('rust'),
    ('postgresql'),
    ('graphql'),
    ('api-design'),
    ('performance'),
    ('beginner');

INSERT INTO public.post_tags (post_id, tag_id) VALUES
    (1, 1), (1, 6),       -- Getting Started with Rust: rust, beginner
    (2, 1),               -- Understanding Ownership: rust
    (3, 2), (3, 5),       -- PostgreSQL Performance Tips: postgresql, performance
    (4, 1),               -- Draft Async Rust: rust
    (5, 3), (5, 4),       -- GraphQL vs REST: graphql, api-design
    (6, 4),               -- Designing Good APIs: api-design
    (8, 3), (8, 2);       -- Turbograph from Scratch: graphql, postgresql

INSERT INTO public.comments (post_id, author_id, body) VALUES
    (1, 2, 'Great intro! The ownership section really clicked for me.'),
    (1, 3, 'Would love a follow-up on lifetimes.'),
    (2, 4, 'Finally understood ownership after reading this, thanks!'),
    (3, 1, 'Partial indexes are also a huge win — worth mentioning.'),
    (5, 2, 'I still prefer REST for simple CRUD, but GraphQL shines for complex queries.'),
    (5, 1, 'Agreed, context matters a lot here.'),
    (8, 2, 'This is exactly what I needed to get started with Turbograph!'),
    (8, 3, 'Love how the schema is derived automatically.');

-- ==============================================================
-- RLS Setup
-- ==============================================================

-- 1. Create a non-superuser role
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user;
    END IF;
END
$$;

-- 2. Revoke and Grant permissions
-- First revoke all for each table for each user (app_user)
REVOKE ALL ON TABLE public.users FROM app_user;
REVOKE ALL ON TABLE public.posts FROM app_user;
REVOKE ALL ON TABLE public.comments FROM app_user;

-- Grant necessary schema usage
GRANT USAGE ON SCHEMA public TO app_user;

-- Grant permissions for RLS tables
GRANT SELECT, INSERT, UPDATE, DELETE ON public.users TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.posts TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.comments TO app_user;

-- Grant permissions on sequences for serial IDs
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- 3. Enable RLS on tables
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.posts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.comments ENABLE ROW LEVEL SECURITY;

-- 4. Define Policies

-- USERS Policy
-- Anyone can create, see. Only owner (id = app.current_user_id) can update/delete.
CREATE POLICY users_policy ON public.users
    FOR ALL
    TO app_user
    USING (true)
    WITH CHECK (
        id = current_setting('app.current_user_id', true)::integer
    );

-- Separate SELECT policy for users if needed, but since USING(true) is for ALL, anyone can see.
-- Wait, FOR ALL with USING(true) means anyone can SELECT, but WITH CHECK only applies to INSERT/UPDATE.
-- However, for DELETE, only USING applies. So we need to be more specific.

DROP POLICY IF EXISTS users_policy ON public.users;

CREATE POLICY users_select_policy ON public.users FOR SELECT TO app_user USING (true);
CREATE POLICY users_insert_policy ON public.users FOR INSERT TO app_user WITH CHECK (true);
CREATE POLICY users_update_policy ON public.users FOR UPDATE TO app_user 
    USING (id = current_setting('app.current_user_id', true)::integer)
    WITH CHECK (id = current_setting('app.current_user_id', true)::integer);
CREATE POLICY users_delete_policy ON public.users FOR DELETE TO app_user 
    USING (id = current_setting('app.current_user_id', true)::integer);

-- POSTS Policy
-- Anyone can create, see. author_id = app.current_user_id can update/delete.
CREATE POLICY posts_select_policy ON public.posts FOR SELECT TO app_user USING (true);
CREATE POLICY posts_insert_policy ON public.posts FOR INSERT TO app_user WITH CHECK (true);
CREATE POLICY posts_update_policy ON public.posts FOR UPDATE TO app_user 
    USING (author_id = current_setting('app.current_user_id', true)::integer)
    WITH CHECK (author_id = current_setting('app.current_user_id', true)::integer);
CREATE POLICY posts_delete_policy ON public.posts FOR DELETE TO app_user 
    USING (author_id = current_setting('app.current_user_id', true)::integer);

-- COMMENTS Policy
-- Anyone can create, see. author_id = app.current_user_id can update/delete.
CREATE POLICY comments_select_policy ON public.comments FOR SELECT TO app_user USING (true);
CREATE POLICY comments_insert_policy ON public.comments FOR INSERT TO app_user WITH CHECK (true);
CREATE POLICY comments_update_policy ON public.comments FOR UPDATE TO app_user 
    USING (author_id = current_setting('app.current_user_id', true)::integer)
    WITH CHECK (author_id = current_setting('app.current_user_id', true)::integer);
CREATE POLICY comments_delete_policy ON public.comments FOR DELETE TO app_user 
    USING (author_id = current_setting('app.current_user_id', true)::integer);
