import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  OneToMany,
} from 'typeorm';
@Entity({ name: 'airdrop_snapshots' })
export class AirdropSnapshot {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: 'varchar', length: 66, unique: true })
  merkleRoot: string; // hex string (0x...)

  @Column({ type: 'int' })
  totalUsers: number; // Should be 1111

  @Column({ type: 'varchar', length: 100 })
  totalTokens: string; // Total tokens in wei

  @Column({ type: 'varchar', length: 100 })
  totalTokensFormatted: string; // Total tokens in human readable format

  @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
  snapshotDate: Date;

  @Column({ type: 'boolean', default: false })
  isFrozen: boolean; // Once frozen, data cannot be updated

  @Column({ type: 'varchar', length: 42, nullable: true })
  contractAddress: string; // Address of deployed airdrop contract (if deployed)

  @Column({ type: 'timestamp', nullable: true })
  deployedAt: Date; // When merkle root was set on contract

  @Column({ type: 'boolean', default: false })
  isActive: boolean; // Only one snapshot can be active at a time

  @CreateDateColumn()
  createdAt: Date;

  // Relations
  @OneToMany('AirdropLeaf', 'snapshot', {
    cascade: true,
    onDelete: 'CASCADE'
  })
  leaves: any[];
}
