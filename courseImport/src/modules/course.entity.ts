import { Entity, Column, PrimaryGeneratedColumn } from 'typeorm';

@Entity()
export class Course {
  @PrimaryGeneratedColumn()
  id!: number;  // Fix: Use `!` to tell TypeScript it's always assigned by TypeORM

  @Column()
  course_title!: string;  // Fix: Use `!` or provide default values

  @Column({ nullable: true })
  set1?: string;  // Fix: Use `?` to make it optional

  @Column({ nullable: true })
  set2?: string;

  @Column({ nullable: true })
  course_do_id?: string;

  constructor(course_title?: string, set1?: string, set2?: string, course_do_id?: string) {
    if (course_title) this.course_title = course_title;
    if (set1) this.set1 = set1;
    if (set2) this.set2 = set2;
    if (course_do_id) this.course_do_id = course_do_id;
  }
}
