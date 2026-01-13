-- SQL Migration: Add collectibleFeeGenerated column to user_brand_votes table
-- This column tracks the fee generated when voting on someone else's collectible (10% of vote cost)

ALTER TABLE user_brand_votes 
ADD COLUMN collectibleFeeGenerated DECIMAL(64, 18) DEFAULT NULL;
