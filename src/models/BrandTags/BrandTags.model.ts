/**
 * @file This file defines the User entity with its properties and methods.
 */
import { Entity, PrimaryGeneratedColumn, ManyToOne } from 'typeorm';

/**
 * @class BrandTags
 * @classdesc BrandTags class represents the tags of a brand in the system.
 */
@Entity({ name: 'brand_tags' })
export class BrandTags {
  @PrimaryGeneratedColumn()
  id: number;

  @ManyToOne('Tag', 'brandTags')
  tag: any;

  @ManyToOne('Brand', 'brandTags')
  brand: any;
}
