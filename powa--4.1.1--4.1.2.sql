-- complain if script is sourced in psql, rather than via CREATE EXTENSION
--\echo Use "ALTER EXTENSION powa" to load this file. \quit

ALTER TABLE public.powa_extensions DROP CONSTRAINT powa_extensions_srvid_fkey;
ALTER TABLE public.powa_extensions ADD
    FOREIGN KEY (srvid) REFERENCES public.powa_servers (id)
    MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;
