import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';
import { User } from '../User';

@Entity({ name: 'airdrop_scores' })
export class AirdropScore {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({
    unique: true,
    nullable: false,
  })
  fid: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 2,
    default: 0,
  })
  basePoints: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  followAccountsMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  channelInteractionMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  holdingBrndMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  collectiblesMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  votedBrandsMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  sharedPodiumsMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  neynarScoreMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  proUserMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 10,
    scale: 4,
    default: 1.0,
  })
  totalMultiplier: number;

  @Column({
    type: 'decimal',
    precision: 15,
    scale: 2,
    default: 0,
  })
  finalScore: number;

  @Column({
    type: 'bigint',
    default: 0,
  })
  tokenAllocation: number;

  @Column({
    type: 'decimal',
    precision: 8,
    scale: 4,
    default: 0,
  })
  percentage: number;

  @Column({
    type: 'json',
    nullable: true,
  })
  challenges: any; // Stores ChallengeBreakdown[] as JSON

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @ManyToOne(() => User, { eager: true })
  @JoinColumn({ name: 'fid', referencedColumnName: 'fid' })
  user: User;
}
