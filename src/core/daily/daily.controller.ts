import { Controller, Get, Post, UseGuards } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';

import { DailyService } from './daily.service';
import { IndexerGuard } from '../../security/guards';
import { logger } from '../../main';

@ApiTags('daily-service')
@Controller('daily-service')
export class DailyController {
  constructor(private readonly dailyService: DailyService) {}

  /**
   * Manual trigger for daily reset (protected by IndexerGuard for security)
   */
  @Post('/trigger-reset')
  @UseGuards(IndexerGuard)
  async triggerManualReset() {
    try {
      logger.log('🔧 [DAILY] Manual reset endpoint triggered');

      await this.dailyService.triggerManualReset();

      return {
        success: true,
        message: 'Daily reset triggered successfully',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      logger.error('❌ [DAILY] Manual reset failed:', error);
      throw new Error(`Daily reset failed: ${error.message}`);
    }
  }

  /**
   * Manual trigger for airdrop leaderboard calculation (protected by IndexerGuard for security)
   */
  @Get('/trigger-airdrop-calculation')
  async triggerManualAirdropCalculation() {
    try {
      logger.log('🔧 [DAILY] Manual airdrop calculation endpoint triggered');

      const result = await this.dailyService.triggerManualAirdropCalculation();

      return {
        success: true,
        message: 'Airdrop leaderboard calculation triggered successfully',
        timestamp: new Date().toISOString(),
        results: {
          eligibleUsers: result.eligibleUsers,
          processed: result.processed,
          successful: result.successful,
          failed: result.failed,
          totalTokensAllocated: result.totalTokensAllocated,
          totalAirdropPoints: result.totalAirdropPoints,
          topUsers: result.topAirdropScores.slice(0, 10), // Return top 10 for response
        },
      };
    } catch (error) {
      logger.error('❌ [DAILY] Manual airdrop calculation failed:', error);
      throw new Error(`Airdrop calculation failed: ${error.message}`);
    }
  }
}
