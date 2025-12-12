import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  ManyToOne,
  JoinColumn,
  Index,
  CreateDateColumn,
} from 'typeorm';
@Entity({ name: 'airdrop_leaves' })
@Index(['snapshotId', 'fid'], { unique: true }) // Ensure one leaf per FID per snapshot
export class AirdropLeaf {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'int' })
  @Index()
  snapshotId: number;

  @Column({ type: 'int' })
  @Index()
  fid: number;

  @Column({ type: 'int' })
  baseAmount: number; // Simple whole number token amount (no decimals)

  @Column({ type: 'varchar', length: 66 })
  leafHash: string; // keccak256(abi.encode(fid, baseAmount))

  @Column({ type: 'decimal', precision: 10, scale: 6 })
  percentage: number; // Percentage of total allocation

  @Column({ type: 'int' })
  leaderboardRank: number; // User's rank in the snapshot

  @Column({ type: 'decimal', precision: 20, scale: 2 })
  finalScore: number; // User's airdrop score at time of snapshot

  @CreateDateColumn()
  createdAt: Date;

  // Relations
  @ManyToOne('AirdropSnapshot', 'leaves', {
    onDelete: 'CASCADE'
  })
  @JoinColumn({ name: 'snapshotId' })
  snapshot: any;
}