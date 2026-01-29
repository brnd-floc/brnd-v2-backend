-- Migration: Create repeat_fee_distributions table
-- Description: Tracks fee distributions to Podium Collectible NFT owners when someone votes
--              for an arrangement that matches their collectible
-- Date: 2025-01-29

CREATE TABLE IF NOT EXISTS `repeat_fee_distributions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `voteTransactionHash` varchar(66) NOT NULL,
  `tokenId` int NOT NULL,
  `brand1Id` int NOT NULL,
  `brand2Id` int NOT NULL,
  `brand3Id` int NOT NULL,
  `voterFid` int NOT NULL,
  `voterWallet` varchar(42) DEFAULT NULL,
  `ownerFid` int NOT NULL,
  `ownerWallet` varchar(42) NOT NULL,
  `feeAmountWei` decimal(65,0) NOT NULL,
  `feeAmountBrnd` decimal(18,2) NOT NULL,
  `voteCostBrnd` decimal(18,2) NOT NULL,
  `status` enum('pending','success','failed') NOT NULL DEFAULT 'pending',
  `transferTxHash` varchar(66) DEFAULT NULL,
  `errorMessage` text DEFAULT NULL,
  `notificationSent` tinyint(1) NOT NULL DEFAULT 0,
  `createdAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `processedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_vote_transaction_hash` (`voteTransactionHash`),
  KEY `IDX_owner_fid` (`ownerFid`),
  KEY `IDX_voter_fid` (`voterFid`),
  KEY `IDX_token_id` (`tokenId`),
  KEY `IDX_status` (`status`),
  KEY `IDX_created_at` (`createdAt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Add foreign key constraints (optional, depends on if you want referential integrity)
-- ALTER TABLE `repeat_fee_distributions` ADD CONSTRAINT `FK_repeat_fee_voter` FOREIGN KEY (`voterFid`) REFERENCES `users` (`fid`);
-- ALTER TABLE `repeat_fee_distributions` ADD CONSTRAINT `FK_repeat_fee_owner` FOREIGN KEY (`ownerFid`) REFERENCES `users` (`fid`);
