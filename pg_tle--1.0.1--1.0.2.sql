/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tle" to load this file. \quit

DROP FUNCTION IF EXISTS pgtle.install_extension (text, text, text, text, text[]);

CREATE FUNCTION pgtle.install_extension
(
  name name,
  version text,
  description text,
  ext text,
  requires text[] DEFAULT NULL
)
RETURNS boolean
SET search_path TO 'pgtle'
AS 'MODULE_PATHNAME', 'pg_tle_install_extension'
LANGUAGE C;

DROP FUNCTION IF EXISTS pgtle.install_update_path(text, text, text, text);

CREATE FUNCTION pgtle.install_update_path
(
  name name,
  fromver text,
  tover text,
  ext text
)
RETURNS boolean
SET search_path TO 'pgtle'
AS 'MODULE_PATHNAME', 'pg_tle_install_update_path'
LANGUAGE C;

DROP FUNCTION IF EXISTS pgtle.set_default_version(text, text);

CREATE FUNCTION pgtle.set_default_version
(
  name name,
  version text
)
RETURNS boolean
SET search_path TO 'pgtle'
AS 'MODULE_PATHNAME', 'pg_tle_set_default_version'
LANGUAGE C;

DROP FUNCTION IF EXISTS pgtle.uninstall_extension(text);

CREATE FUNCTION pgtle.uninstall_extension(name name)
RETURNS boolean
SET search_path TO 'pgtle'
AS $_pgtleie_$
  DECLARE
    ctrpattern text;
    sqlpattern text;
    searchsql  text;
    dropsql    text;
    pgtlensp    text := 'pgtle';
    func       text;
    existsvar  record;
  BEGIN

    ctrpattern := format('%s%%.control', name);
    sqlpattern := format('%s%%.sql', name);
    searchsql := 'SELECT proname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace WHERE proname LIKE $1 AND n.nspname OPERATOR(pg_catalog.=) $2';

    EXECUTE searchsql USING ctrpattern, pgtlensp INTO existsvar;
    IF existsvar IS NULL THEN
      RAISE EXCEPTION 'Extension % does not exist', name USING ERRCODE = 'no_data_found';
    ELSE
      FOR func IN EXECUTE searchsql USING ctrpattern, pgtlensp LOOP
        dropsql := format('DROP FUNCTION %I()', func);
        EXECUTE dropsql;
      END LOOP;
    END IF;

    EXECUTE searchsql USING sqlpattern, pgtlensp INTO existsvar;
    IF existsvar IS NULL THEN
      RAISE WARNING 'Extension % has an anomaly; control function exists, but no sql commands function exists', name;
    ELSE
      FOR func IN EXECUTE searchsql USING sqlpattern, pgtlensp LOOP
        dropsql := format('DROP FUNCTION %I()', func);
        EXECUTE dropsql;
      END LOOP;
    END IF;

    RETURN true;
  END;
$_pgtleie_$
LANGUAGE plpgsql STRICT;

DROP FUNCTION IF EXISTS pgtle.uninstall_extension_if_exists(text);

CREATE FUNCTION pgtle.uninstall_extension_if_exists(name name)
RETURNS boolean
SET search_path TO 'pgtle'
AS $_pgtleie_$
BEGIN
  PERFORM pgtle.uninstall_extension(name);
  RETURN TRUE;
EXCEPTION
  WHEN no_data_found THEN
    RETURN FALSE;
END;
$_pgtleie_$
LANGUAGE plpgsql STRICT;

-- uninstall an extension for a specific version
DROP FUNCTION IF EXISTS pgtle.uninstall_extension(text, text);

CREATE FUNCTION pgtle.uninstall_extension(name name, version text)
RETURNS boolean
SET search_path TO 'pgtle'
AS $_pgtleie_$
  DECLARE
    sqlpattern text;
    searchsql  text;
    dropsql    text;
    pgtlensp   text := 'pgtle';
    func       text;
    row_count  bigint;
  BEGIN
    sqlpattern := format('%s--%%%s%%.sql', name, version);
    searchsql := 'SELECT proname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace WHERE proname LIKE $1 AND n.nspname OPERATOR(pg_catalog.=) $2';

    FOR func IN EXECUTE searchsql USING sqlpattern, pgtlensp LOOP
      dropsql := format('DROP FUNCTION %I()', func);
      EXECUTE dropsql;
    END LOOP;

    RETURN TRUE;
  END;
$_pgtleie_$
LANGUAGE plpgsql STRICT;

-- uninstall a specific update path
DROP FUNCTION IF EXISTS pgtle.uninstall_update_path(text, text, text);

CREATE FUNCTION pgtle.uninstall_update_path(name name, fromver text, tover text)
RETURNS boolean
SET search_path TO 'pgtle'
AS $_pgtleie_$
  DECLARE
    sqlpattern text;
    searchsql  text;
    dropsql    text;
    pgtlensp   text := 'pgtle';
    func       text;
    existsvar  record;
  BEGIN
    sqlpattern := format('%s--%s--%s.sql', name, fromver, tover);
    searchsql := 'SELECT proname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace WHERE proname OPERATOR(pg_catalog.=) $1 AND n.nspname OPERATOR(pg_catalog.=) $2';

    EXECUTE searchsql USING sqlpattern, pgtlensp INTO existsvar;

    IF existsvar IS NULL THEN
      RAISE EXCEPTION 'Extension % does not exist', name USING ERRCODE = 'no_data_found';
    ELSE
      FOR func IN EXECUTE searchsql USING sqlpattern, pgtlensp LOOP
        dropsql := format('DROP FUNCTION %I()', func);
        EXECUTE dropsql;
      END LOOP;
    END IF;

    RETURN TRUE;
  END;
$_pgtleie_$
LANGUAGE plpgsql STRICT;

DROP FUNCTION IF EXISTS pgtle.uninstall_update_path_if_exists(text, text, text);

CREATE FUNCTION pgtle.uninstall_update_path_if_exists(name name, fromver text, tover text)
RETURNS boolean
SET search_path TO 'pgtle'
AS $_pgtleie_$
BEGIN
  PERFORM pgtle.uninstall_update_path(name, fromver, tover);
  RETURN TRUE;
EXCEPTION
  WHEN no_data_found THEN
    RETURN FALSE;
END;
$_pgtleie_$
LANGUAGE plpgsql STRICT;

-- Revoke privs from PUBLIC
REVOKE EXECUTE ON FUNCTION pgtle.install_extension
(
  name name,
  version text,
  description text,
  ext text,
  requires text[]
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.install_update_path
(
  name name,
  fromver text,
  tover text,
  ext text
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.set_default_version
(
  name name,
  version text
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.uninstall_extension
(
  name name
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.uninstall_extension
(
  name name,
  version text
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.uninstall_extension_if_exists
(
  name name
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.uninstall_update_path
(
  name name,
  fromver text,
  tover text
) FROM PUBLIC;

REVOKE EXECUTE ON FUNCTION pgtle.uninstall_update_path_if_exists
(
  name name,
  fromver text,
  tover text
) FROM PUBLIC;

DO
$_do_$
BEGIN
   IF EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'pgtle_admin') THEN

      RAISE NOTICE 'Role "pgtle_admin" already exists. Skipping.';
   ELSE
      CREATE ROLE pgtle_admin NOLOGIN;
   END IF;
END
$_do_$;

GRANT USAGE, CREATE ON SCHEMA pgtle TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.install_extension
(
  name name,
  version text,
  description text,
  ext text,
  requires text[]
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.install_update_path
(
  name name,
  fromver text,
  tover text,
  ext text
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.set_default_version
(
  name name,
  version text
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.uninstall_extension
(
  name name
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.uninstall_extension
(
  name name,
  version text
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.uninstall_extension_if_exists
(
  name name
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.uninstall_update_path
(
  name name,
  fromver text,
  tover text
) TO pgtle_admin;

GRANT EXECUTE ON FUNCTION pgtle.uninstall_update_path_if_exists
(
  name name,
  fromver text,
  tover text
) TO pgtle_admin;

--CREATE TABLE pgtle.feature_info(
--	feature pgtle.pg_tle_features,
--	schema_name text,
--	proname text,
--	obj_identity text NOT NULL,
--  PRIMARY KEY(feature, schema_name, proname));

--SELECT pg_catalog.pg_extension_config_dump('pgtle.feature_info', '');

--GRANT SELECT on pgtle.feature_info TO PUBLIC;

-- Helper function to register features in the feature_info table
--CREATE FUNCTION pgtle.register_feature(proc regproc, feature pgtle.pg_tle_features)
--RETURNS VOID
--LANGUAGE plpgsql
--AS $$
--DECLARE
--pg_proc_relid oid;
--proc_oid oid;
--schema_name text;
--nspoid oid;
--proname text;
--proc_schema_name text;
--ident text;

--BEGIN
--	SELECT oid into nspoid FROM pg_catalog.pg_namespace
--	where nspname OPERATOR(pg_catalog.=) 'pg_catalog';
--
--	SELECT oid into pg_proc_relid from pg_catalog.pg_class
--	where relname OPERATOR(pg_catalog.=) 'pg_proc' and relnamespace OPERATOR(pg_catalog.=) nspoid;
--
--	SELECT pg_namespace.nspname, pg_proc.oid, pg_proc.proname into proc_schema_name, proc_oid, proname FROM
--	pg_catalog.pg_namespace, pg_catalog.pg_proc
--	where pg_proc.oid OPERATOR(pg_catalog.=) proc AND pg_proc.pronamespace OPERATOR(pg_catalog.=) pg_namespace.oid;
--
--	SELECT identity into ident FROM pg_catalog.pg_identify_object(pg_proc_relid, proc_oid, 0);
--
--	INSERT INTO pgtle.feature_info VALUES (feature, proc_schema_name, proname, ident);
--END;
--$$;

-- Helper function to softly fail if we try to register a function that already exists
--CREATE FUNCTION pgtle.register_feature_if_not_exists(proc regproc, feature pgtle.pg_tle_features)
--RETURNS bool
--LANGUAGE plpgsql
--AS $$
--BEGIN
--  PERFORM pgtle.register_feature(proc, feature);
--  RETURN TRUE;
--EXCEPTION
--  -- only catch the unique violation. let all other exceptions pass through.
--  WHEN unique_violation THEN
--    RETURN FALSE;
--END;
--$$;

-- Helper function to delete from table
--CREATE FUNCTION pgtle.unregister_feature(proc regproc, feature pgtle.pg_tle_features)
--RETURNS void
--LANGUAGE plpgsql
--AS $$
--DECLARE
--	pg_proc_relid oid;
--	proc_oid oid;
--	schema_name text;
--	nspoid oid;
--	proc_name text;
--	proc_schema_name text;
--	ident text;
--	row_count bigint;
--BEGIN
--	SELECT oid into nspoid
--  FROM pg_catalog.pg_namespace
--	WHERE nspname OPERATOR(pg_catalog.=) 'pg_catalog';
--
--	SELECT oid into pg_proc_relid
--  FROM pg_catalog.pg_class
--	WHERE
--		relname OPERATOR(pg_catalog.=) 'pg_proc' AND
--		relnamespace OPERATOR(pg_catalog.=) nspoid;
--
--	SELECT
--		pg_namespace.nspname,
--		pg_proc.oid,
--		pg_proc.proname
--  INTO
--		proc_schema_name,
--		proc_oid,
--		proc_name
--	FROM pg_catalog.pg_namespace, pg_catalog.pg_proc
--	WHERE
--		pg_proc.oid OPERATOR(pg_catalog.=) proc AND
--		pg_proc.pronamespace OPERATOR(pg_catalog.=) pg_namespace.oid;
--
--	DELETE FROM pgtle.feature_info
--	WHERE
--		feature_info.feature OPERATOR(pg_catalog.=) $2 AND
--		feature_info.schema_name OPERATOR(pg_catalog.=) proc_schema_name AND
--		feature_info.proname OPERATOR(pg_catalog.=) proc_name;
--
--	GET DIAGNOSTICS row_count := ROW_COUNT;
--
--	IF ROW_COUNT = 0 THEN
--    RAISE EXCEPTION 'Could not unregister "%": does not exist.', $1 USING ERRCODE = 'no_data_found';
--  END IF;
--END;
--$$;

-- Helper to softly fail if we try to unregister a function that does not exist
--CREATE FUNCTION pgtle.unregister_feature_if_exists(proc regproc, feature pgtle.pg_tle_features)
--RETURNS bool
--LANGUAGE plpgsql
--AS $$
--BEGIN
--  PERFORM pgtle.unregister_feature(proc, feature);
--  RETURN TRUE;
--EXCEPTION
--  -- only catch the error that no data was found
--  WHEN no_data_found THEN
--    RETURN FALSE;
--END;
--$$;

-- Revoke privs from PUBLIC
--REVOKE EXECUTE ON FUNCTION pgtle.register_feature
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) FROM PUBLIC;

--REVOKE EXECUTE ON FUNCTION pgtle.register_feature_if_not_exists
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) FROM PUBLIC;

--REVOKE EXECUTE ON FUNCTION pgtle.unregister_feature
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) FROM PUBLIC;

--REVOKE EXECUTE ON FUNCTION pgtle.unregister_feature_if_exists
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) FROM PUBLIC;

-- Grant privs to pgtle_admin
--GRANT EXECUTE ON FUNCTION pgtle.register_feature
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) TO pgtle_admin;

--GRANT EXECUTE ON FUNCTION pgtle.register_feature_if_not_exists
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) TO pgtle_admin;

--GRANT EXECUTE ON FUNCTION pgtle.unregister_feature
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) TO pgtle_admin;

--GRANT EXECUTE ON FUNCTION pgtle.unregister_feature_if_exists
--(
--  proc regproc,
--  feature pgtle.pg_tle_features
--) TO pgtle_admin;

--REVOKE ALL ON SCHEMA pgtle FROM PUBLIC;
--GRANT USAGE ON SCHEMA pgtle TO PUBLIC;
--GRANT INSERT,DELETE ON TABLE pgtle.feature_info TO pgtle_admin;

-- Prevent function from being dropped if referenced in table

CREATE OR REPLACE FUNCTION pgtle.pg_tle_feature_info_schema_rename()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
  obj RECORD;
  num_rows int;
  schname text;

BEGIN
	FOR obj IN SELECT * FROM pg_catalog.pg_event_trigger_ddl_commands()

	LOOP
  select current_query() into schname;
    RAISE NOTICE '% current_query %', tg_tag, schname;
  SELECT nspname INTO schname from pg_catalog.pg_namespace where oid = obj.objid;
    RAISE NOTICE '% nspname %', tg_tag, schname;
    RAISE NOTICE '% classid %',  tg_tag, obj.classid;
    RAISE NOTICE '% objid %',  tg_tag, obj.objid;
    RAISE NOTICE '% objsubid %',  tg_tag, obj.objsubid;
    RAISE NOTICE '% command_tag %',  tg_tag, obj.command_tag;
    RAISE NOTICE '% object_type %',  tg_tag, obj.object_type;
    RAISE NOTICE '% schema_name %',  tg_tag, obj.schema_name;
    RAISE NOTICE '% object_identity %',  tg_tag, obj.object_identity;
    RAISE NOTICE '% in_extension %',  tg_tag, obj.in_extension;
    -- RAISE NOTICE '% command %',  tg_tag, obj.command;
	END LOOP;
  if tg_tag = 'ALTER SCHEMA' then
  RAISE EXCEPTION 'Force Fail';
  end if;
END;
$$;

CREATE EVENT TRIGGER pg_tle_event_trigger_for_schema_rename_pre
   ON ddl_command_start
   EXECUTE FUNCTION pgtle.pg_tle_feature_info_schema_rename();
CREATE EVENT TRIGGER pg_tle_event_trigger_for_schema_rename_post
   ON ddl_command_end
   EXECUTE FUNCTION pgtle.pg_tle_feature_info_schema_rename();

