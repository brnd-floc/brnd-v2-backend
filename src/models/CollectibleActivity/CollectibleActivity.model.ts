import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  ManyToOne,
  JoinColumn,
  Index,
} from 'typeorm';

@Entity({ name: 'collectible_activity' })
export class CollectibleActivity {
  @PrimaryGeneratedColumn()
  id: number;

  @Index()
  @Column()
  tokenId: number;

  @Column()
  eventType: string; // 'mint' | 'sale'

  @Column({ type: 'decimal', precision: 65, scale: 0, nullable: true })
  price: string;

  @Column()
  fromFid: number;

  @Column({ nullable: true })
  toFid: number;

  @Column({ length: 42 })
  fromWallet: string;

  @Column({ nullable: true, length: 42 })
  toWallet: string;

  @Column({ length: 66 })
  txHash: string;

  @Column({ type: 'bigint' })
  timestamp: number;

  @ManyToOne('User', { nullable: true })
  @JoinColumn({ name: 'fromFid', referencedColumnName: 'fid' })
  fromUser: any;

  @ManyToOne('User', { nullable: true })
  @JoinColumn({ name: 'toFid', referencedColumnName: 'fid' })
  toUser: any;
}
