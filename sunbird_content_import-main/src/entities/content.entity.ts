import { Entity, Column, PrimaryColumn } from 'typeorm';

@Entity('content_imports') // Maps to the updated table name
export class Content {
  @PrimaryColumn({ type: 'char', length: 36 })
  //content_id!: string; // Primary key
  id!: string; // Primary key

  @Column({ type: 'varchar', length: 255, nullable: true })
  program?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  domain?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  sub_domain?: string;

  @Column({ type: 'varchar', length: 50, nullable: true })
  content_language?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  primary_user?: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  target_age_group?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  cont_title?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  cont_engtitle?: string;

  @Column({ type: 'text', nullable: true })
  cont_url?: string;

  @Column({ type: 'text', nullable: true })
  cont_dwurl?: string;

  @Column({ type: 'text', nullable: true })
  type?: string;

  @Column({ type: 'text', nullable: true })
  resource_desc?: string;

  @Column({ type: 'text', nullable: true })
  subjects?: string;

  @Column({ type: 'text', nullable: true })
  course_keywords?: string;

  @Column({ type: 'tinyint', default: 0 })
  migrated?: number; // Flag to track migration status

  @Column({ type: 'varchar', length: 1000, nullable: true })
  do_id?: string; // do_id for migrated content

  @Column({ type: 'tinyint', default: 0 })
  convertedFileflag?: number; // Flag for converted files

  @Column({ type: 'text', nullable: true })
  convertedUrl?: string; // URL for converted files

  @Column({ type: 'varchar', length: 255, nullable: true })
  cont_thumb?: string;

  @Column({ type: 'varchar', length: 255, nullable: true })
  old_system_content_id?: string;

  @Column({ type: 'varchar', length: 100, nullable: true })
  comment?: string;
}
