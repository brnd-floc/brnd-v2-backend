/**
 * @file RepeatFeeDistribution model for tracking fee distributions to podium collectible owners
 * When someone votes for an arrangement that exists as a Podium Collectible NFT,
 * the owner earns 10% of the vote cost, paid from the treasury.
 */
import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  ManyToOne,
  JoinColumn,
  Index,
  CreateDateColumn,
} from 'typeorm';

@Entity({ name: 'repeat_fee_distributions' })
export class RepeatFeeDistribution {
  @PrimaryGeneratedColumn()
  id: number;

  // The vote transaction hash that triggered this fee distribution
  // Used for idempotency - we only process each vote once
  @Index({ unique: true })
  @Column({ length: 66 })
  voteTransactionHash: string;

  // The podium collectible token ID
  @Column()
  tokenId: number;

  // Brand IDs from the vote (for reference)
  @Column()
  brand1Id: number;

  @Column()
  brand2Id: number;

  @Column()
  brand3Id: number;

  // Voter info
  @Column()
  voterFid: number;

  @Column({ length: 42, nullable: true })
  voterWallet: string;

  // Recipient info (podium owner)
  @Column()
  ownerFid: number;

  @Column({ length: 42 })
  ownerWallet: string;

  // Fee amount in wei (stored as string for precision)
  @Column({ type: 'decimal', precision: 65, scale: 0 })
  feeAmountWei: string;

  // Fee amount in whole BRND (for easy querying)
  @Column({ type: 'decimal', precision: 18, scale: 2 })
  feeAmountBrnd: number;

  // Vote cost that the fee was calculated from (in BRND)
  @Column({ type: 'decimal', precision: 18, scale: 2 })
  voteCostBrnd: number;

  // Transfer status
  @Column({
    type: 'enum',
    enum: ['pending', 'success', 'failed'],
    default: 'pending',
  })
  status: 'pending' | 'success' | 'failed';

  // Transfer transaction hash (if successful)
  @Column({ length: 66, nullable: true })
  transferTxHash: string;

  // Error message if transfer failed
  @Column({ type: 'text', nullable: true })
  errorMessage: string;

  // Notification sent to owner
  @Column({ default: false })
  notificationSent: boolean;

  @CreateDateColumn()
  createdAt: Date;

  @Column({ nullable: true })
  processedAt: Date;

  // Relationships for easy querying
  @ManyToOne('User', { nullable: true })
  @JoinColumn({ name: 'voterFid', referencedColumnName: 'fid' })
  voter: any;

  @ManyToOne('User', { nullable: true })
  @JoinColumn({ name: 'ownerFid', referencedColumnName: 'fid' })
  owner: any;
}
