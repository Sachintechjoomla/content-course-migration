import { Controller, Get } from '@nestjs/common';
import { CronService } from './cron.service';

@Controller('cron')
export class CronController {
  constructor(private readonly cronService: CronService) {}

  @Get('trigger')
  async triggerCronJob() {

    await this.cronService.processRecords();
    // return { message: 'Cron job triggered successfully.' };
  }

  @Get('downloadFile')
  async downloadFile() {
    await this.cronService.downloadFile();
    return { message: 'Cron job triggered successfully.' };
  }

  @Get('checkDataValidAndUpdate')
  async fixDatabaseData() {

    await this.cronService.checkDataValidAndUpdate();
    // return { message: 'Cron job checkDataValidAndUpdate successfully.' };
  }


  @Get('updateRecordsChangeCreatedBy')
  async updateRecordsChangeCreatedBy() {

    await this.cronService.updateRecordsChangeCreatedBy();
    return { message: 'Cron job updateRecordsChangeCreatedBy successfully.' };
  }

  @Get("delete-all")
  async deleteAllContents() {
   // console.log("Importing courses...");
   return this.cronService.deleteAllContents();
  }
}

