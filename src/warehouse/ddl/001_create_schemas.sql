-- ============================================================================
-- ChatGPT Data Platform - Schema Setup
-- ============================================================================
-- Run this first to create the database and all schemas.

CREATE DATABASE IF NOT EXISTS CHATGPT_PLATFORM;
USE DATABASE CHATGPT_PLATFORM;

-- Raw event landing zone (from Kafka/staging)
CREATE SCHEMA IF NOT EXISTS RAW;

-- Validated and deduplicated data
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Star schema: facts + dimensions
CREATE SCHEMA IF NOT EXISTS WAREHOUSE;

-- Business metric tables (DAU, engagement, revenue)
CREATE SCHEMA IF NOT EXISTS CANONICAL;

-- Trust & safety data (abuse detection, quarantine)
CREATE SCHEMA IF NOT EXISTS SAFETY;

-- Research team exports (model training data)
CREATE SCHEMA IF NOT EXISTS EXPORT;
