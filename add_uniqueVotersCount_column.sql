-- SQL Migration: Add uniqueVotersCount column to brands table
-- This column tracks the number of unique voters for each brand

ALTER TABLE brands 
ADD COLUMN uniqueVotersCount INT DEFAULT 0;


