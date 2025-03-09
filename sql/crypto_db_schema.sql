--
-- PostgreSQL database dump
--

-- Dumped from database version 14.16
-- Dumped by pg_dump version 14.16

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: telegram_groups_group_id_seq; Type: SEQUENCE; Schema: public; Owner: bot
--

CREATE SEQUENCE public.telegram_groups_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.telegram_groups_group_id_seq OWNER TO bot;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: telegram_groups; Type: TABLE; Schema: public; Owner: bot
--

CREATE TABLE public.telegram_groups (
    group_id bigint DEFAULT nextval('public.telegram_groups_group_id_seq'::regclass) NOT NULL,
    telegram_id bigint NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.telegram_groups OWNER TO bot;

--
-- Name: TABLE telegram_groups; Type: COMMENT; Schema: public; Owner: bot
--

COMMENT ON TABLE public.telegram_groups IS 'Almacena los grupos de Telegram monitoreados por el bot.';


--
-- Name: telegram_messages_message_id_seq; Type: SEQUENCE; Schema: public; Owner: bot
--

CREATE SEQUENCE public.telegram_messages_message_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.telegram_messages_message_id_seq OWNER TO bot;

--
-- Name: telegram_messages; Type: TABLE; Schema: public; Owner: bot
--

CREATE TABLE public.telegram_messages (
    message_id bigint DEFAULT nextval('public.telegram_messages_message_id_seq'::regclass) NOT NULL,
    group_id bigint NOT NULL,
    message_timestamp timestamp with time zone NOT NULL,
    raw_text text NOT NULL,
    sender_id bigint NOT NULL,
    is_call boolean DEFAULT false NOT NULL,
    reply_to_message_id bigint,
    token_id bigint,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.telegram_messages OWNER TO bot;

--
-- Name: TABLE telegram_messages; Type: COMMENT; Schema: public; Owner: bot
--

COMMENT ON TABLE public.telegram_messages IS 'Registra todos los mensajes capturados de los grupos de Telegram.';


--
-- Name: token_calls_call_id_seq; Type: SEQUENCE; Schema: public; Owner: bot
--

CREATE SEQUENCE public.token_calls_call_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.token_calls_call_id_seq OWNER TO bot;

--
-- Name: token_calls; Type: TABLE; Schema: public; Owner: bot
--

CREATE TABLE public.token_calls (
    call_id bigint DEFAULT nextval('public.token_calls_call_id_seq'::regclass) NOT NULL,
    token_id bigint NOT NULL,
    call_timestamp timestamp with time zone NOT NULL,
    call_price numeric(30,10) NOT NULL,
    message_id bigint,
    note text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.token_calls OWNER TO bot;

--
-- Name: TABLE token_calls; Type: COMMENT; Schema: public; Owner: bot
--

COMMENT ON TABLE public.token_calls IS 'Almacena los calls específicos de tokens detectados en mensajes.';


--
-- Name: tokens_token_id_seq; Type: SEQUENCE; Schema: public; Owner: bot
--

CREATE SEQUENCE public.tokens_token_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tokens_token_id_seq OWNER TO bot;

--
-- Name: tokens; Type: TABLE; Schema: public; Owner: bot
--

CREATE TABLE public.tokens (
    token_id bigint DEFAULT nextval('public.tokens_token_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    ticker text NOT NULL,
    blockchain text NOT NULL,
    contract_address text NOT NULL,
    dex text,
    first_call_liquidity numeric(30,10),
    supply numeric(30,10) NOT NULL,
    initial_call_timestamp timestamp with time zone DEFAULT now(),
    group_call text,
    call_price numeric(30,10) NOT NULL,
    token_age integer,
    dexscreener_url text
);


ALTER TABLE public.tokens OWNER TO bot;

--
-- Name: TABLE tokens; Type: COMMENT; Schema: public; Owner: bot
--

COMMENT ON TABLE public.tokens IS 'Registra los tokens detectados en los mensajes de Telegram, con soporte para cualquier blockchain.';


--
-- Name: telegram_groups telegram_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.telegram_groups
    ADD CONSTRAINT telegram_groups_pkey PRIMARY KEY (group_id);


--
-- Name: telegram_groups telegram_groups_telegram_id_key; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.telegram_groups
    ADD CONSTRAINT telegram_groups_telegram_id_key UNIQUE (telegram_id);


--
-- Name: telegram_messages telegram_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.telegram_messages
    ADD CONSTRAINT telegram_messages_pkey PRIMARY KEY (message_id);


--
-- Name: token_calls token_calls_pkey; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.token_calls
    ADD CONSTRAINT token_calls_pkey PRIMARY KEY (call_id);


--
-- Name: tokens tokens_contract_address_key; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_contract_address_key UNIQUE (contract_address);


--
-- Name: tokens tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_pkey PRIMARY KEY (token_id);


--
-- Name: idx_telegram_groups_telegram_id; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_telegram_groups_telegram_id ON public.telegram_groups USING btree (telegram_id);


--
-- Name: idx_telegram_msg_group; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_telegram_msg_group ON public.telegram_messages USING btree (group_id);


--
-- Name: idx_telegram_msg_time; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_telegram_msg_time ON public.telegram_messages USING btree (message_timestamp DESC);


--
-- Name: idx_token_calls_time; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_token_calls_time ON public.token_calls USING btree (call_timestamp DESC);


--
-- Name: idx_token_calls_token; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_token_calls_token ON public.token_calls USING btree (token_id);


--
-- Name: idx_tokens_blockchain; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_tokens_blockchain ON public.tokens USING btree (blockchain);


--
-- Name: idx_tokens_contract; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_tokens_contract ON public.tokens USING btree (contract_address);


--
-- Name: idx_tokens_ticker; Type: INDEX; Schema: public; Owner: bot
--

CREATE INDEX idx_tokens_ticker ON public.tokens USING btree (ticker);


--
-- Name: telegram_messages telegram_messages_group_fk; Type: FK CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.telegram_messages
    ADD CONSTRAINT telegram_messages_group_fk FOREIGN KEY (group_id) REFERENCES public.telegram_groups(group_id);


--
-- Name: telegram_messages telegram_messages_token_fk; Type: FK CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.telegram_messages
    ADD CONSTRAINT telegram_messages_token_fk FOREIGN KEY (token_id) REFERENCES public.tokens(token_id);


--
-- Name: token_calls token_calls_msg_fk; Type: FK CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.token_calls
    ADD CONSTRAINT token_calls_msg_fk FOREIGN KEY (message_id) REFERENCES public.telegram_messages(message_id);


--
-- Name: token_calls token_calls_token_fk; Type: FK CONSTRAINT; Schema: public; Owner: bot
--

ALTER TABLE ONLY public.token_calls
    ADD CONSTRAINT token_calls_token_fk FOREIGN KEY (token_id) REFERENCES public.tokens(token_id);


--
-- PostgreSQL database dump complete
--

