import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CronService } from './cron.service';
import { CronController } from './cron.controller';
import { Content } from '../entities/content.entity';
import { ContentModule } from '../content/content.module';

@Module({
  imports: [
    TypeOrmModule.forFeature([Content]), // Register Content entity
    ContentModule,                      // Import ContentModule
  ],
  controllers: [CronController],
  providers: [CronService],
})
export class CronModule {}
