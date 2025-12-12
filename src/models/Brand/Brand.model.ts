/**
 * @file This file defines the User entity with its properties and methods.
 */
import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
  ManyToOne,
  Unique,
} from 'typeorm';

/**
 * @class Brand
 * @classdesc Brand class represents a brand in the system.
 */
@Entity({ name: 'brands' })
@Unique(['name'])
export class Brand {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @Column()
  url: string;

  @Column()
  warpcastUrl: string;

  @Column({ length: 4096 })
  description: string;

  @ManyToOne('Category', 'brands')
  category: any;

  @Column()
  followerCount: number;

  @Column()
  imageUrl: string;

  @Column()
  profile: string;

  @Column()
  channel: string;

  @Column()
  ranking: string;

  @Column()
  score: number;

  @Column()
  stateScore: number;

  @Column()
  scoreWeek: number;

  @Column()
  stateScoreWeek: number;

  @Column({
    default: 0,
  })
  rankingWeek: number;

  @Column({
    default: 0,
  })
  scoreMonth: number;

  @Column({
    default: 0,
  })
  stateScoreMonth: number;

  @Column({
    default: 0,
  })
  rankingMonth: number;

  @Column({
    default: 0,
  })
  bonusPoints: number;

  @Column({
    default: 0,
  })
  banned: number;

  @Column()
  queryType: number; // 0: Channel, 1: Profile

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @Column({
    default: 0,
  })
  currentRanking: number;

  // V3 Contract Integration Fields
  @Column({ nullable: true, length: 42 })
  walletAddress: string;

  @Column({ type: 'decimal', precision: 36, scale: 18, default: '0' })
  totalBrndAwarded: string;

  @Column({ type: 'decimal', precision: 36, scale: 18, default: '0' })
  availableBrnd: string;

  @Column({ nullable: true })
  onChainCreatedAt: Date;

  @Column({ nullable: true, unique: true })
  onChainId: number;

  @Column({ nullable: true })
  onChainFid: number;

  @Column({ nullable: true })
  onChainHandle: string;

  @Column({ nullable: true, length: 42 })
  onChainWalletAddress: string;

  @Column({ nullable: true })
  metadataHash: string;

  // Contract Upload Tracking
  @Column({ default: false })
  isUploadedToContract: boolean;

  @OneToMany('UserBrandVotes', 'brand1')
  userBrandVotes1: any[];

  @OneToMany('UserBrandVotes', 'brand2')
  userBrandVotes2: any[];

  @OneToMany('UserBrandVotes', 'brand3')
  userBrandVotes3: any[];

  @OneToMany('BrandTags', 'brand')
  brandTags: any[];

  @Column({ nullable: true })
  founderFid: number;

  @Column({ nullable: true })
  ticker: string;

  @Column({ nullable: true })
  contractAddress: string;
}
