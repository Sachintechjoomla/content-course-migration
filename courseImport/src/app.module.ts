import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { CourseModule } from "./modules/course.module";
import { ConfigModule } from "@nestjs/config";
import { HttpModule } from "@nestjs/axios";

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),  // ✅ Make ConfigModule global
    TypeOrmModule.forRoot({
      type: "mysql",
      host: process.env.DB_HOST ?? "localhost",
      port: Number(process.env.DB_PORT) || 3306,
      username: process.env.DB_USER ?? "root",
      password: process.env.DB_PASSWORD ?? "password",
      database: process.env.DB_NAME ?? "migrated_content",
      autoLoadEntities: true,
      synchronize: true,
    }),
    HttpModule,
    CourseModule,
  ],
})
export class AppModule {}
