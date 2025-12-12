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
} from 'typeorm';

// Types
import { UserRoleEnum } from './User.types';
import type { Brand } from '../Brand/Brand.model';

/**
 * @class User
 * @classdesc User class represents a user in the system.
 */
@Entity({ name: 'users' })
export class User {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({
    unique: true,
    nullable: false,
  })
  fid: number;

  @Column()
  username: string;

  @Column({
    default: null,
    nullable: true,
  })
  photoUrl: string;

  @Column({
    default: 0,
  })
  points: number;

  @Column({
    type: 'enum',
    enum: UserRoleEnum,
    default: UserRoleEnum.USER,
  })
  role: UserRoleEnum;

  @Column({
    default: 0,
  })
  dailyStreak: number;

  @Column({
    default: null,
    nullable: true,
  })
  maxDailyStreak: number;

  @Column({
    default: 0,
  })
  totalPodiums: number;

  @Column({
    default: 0,
  })
  votedBrandsCount: number;

  @Column({
    default: 0,
  })
  brndPowerLevel: number;

  @Column({
    default: 0,
  })
  totalVotes: number;

  @Column({
    default: 0,
  })
  lastVoteDay: number;

  @Column({
    nullable: true,
  })
  lastVoteTimestamp: Date;

  @Column({
    nullable: true,
  })
  address: string;

  @Column({
    default: false,
  })
  banned: boolean;

  @Column({
    default: 0,
  })
  powerups: number;

  @Column({
    default: false,
  })
  verified: boolean;

  @ManyToOne('Brand', {
    nullable: true,
  })
  favoriteBrand: Brand | null;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @Column({
    default: false,
  })
  notificationsEnabled: boolean;

  @Column({
    default: null,
    nullable: true,
  })
  notificationToken: string;

  @Column({
    default: null,
    nullable: true,
  })
  lastVoteReminderSent: Date;

  @Column({
    default: 0.0,
  })
  neynarScore: number;

  @OneToMany('UserBrandVotes', 'user')
  userBrandVotes: any[];
}
