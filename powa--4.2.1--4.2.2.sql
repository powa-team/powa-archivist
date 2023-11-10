-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION powa" to load this file. \quit

-- Nothing to do, 4.2.0 and 4.2.1 are identical, the new version was only
-- needed to fix a bug with the 4.1.4 <-> 4.2.0 upgrade script.
