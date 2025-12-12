// src/core/embeds/embeds.controller.ts

import {
  Controller,
  Get,
  Param,
  Res,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { Response } from 'express';
import { EmbedsService } from './services';
import { hasError } from '../../utils';

@ApiTags('embeds')
@Controller('embeds')
export class EmbedsController {
  private readonly logger = new Logger(EmbedsController.name);

  constructor(private readonly embedsService: EmbedsService) {}

  /**
   * Generate dynamic embed for podium sharing
   * URL: /embeds/podium/:voteId
   */
  @Get('/podium/:voteId')
  async getPodiumEmbed(
    @Param('voteId') voteId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`Generating podium embed for vote ID: ${voteId}`);

      const embedHtml = await this.embedsService.generatePodiumEmbed(voteId);

      if (!embedHtml) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getPodiumEmbed',
          'Podium not found',
        );
      }

      // Return HTML with proper content-type
      res.setHeader('Content-Type', 'text/html');
      return res.send(embedHtml);
    } catch (error) {
      this.logger.error(`Error generating podium embed for ${voteId}:`, error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getPodiumEmbed',
        error.message,
      );
    }
  }

  /**
   * Generate podium image (PNG)
   * URL: /embeds/podium/:voteId/image
   */
  @Get('/podium/:voteId/image')
  async getPodiumImage(
    @Param('voteId') voteId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`Generating podium image for vote ID: ${voteId}`);

      const imageHtml =
        await this.embedsService.generatePodiumImageHtml(voteId);

      if (!imageHtml) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getPodiumImage',
          'Podium not found',
        );
      }

      // Return HTML that renders as an image
      res.setHeader('Content-Type', 'text/html');
      res.setHeader('Cache-Control', 'public, max-age=3600'); // Cache for 1 hour
      return res.send(imageHtml);
    } catch (error) {
      this.logger.error(`Error generating podium image for ${voteId}:`, error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getPodiumImage',
        error.message,
      );
    }
  }

  /**
   * Generate brand image (PNG)
   * URL: /embeds/brand/:brandId/image
   */
  @Get('/brand/:brandId/image')
  async getBrandImage(
    @Param('brandId') brandId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`Generating brand image for brand ID: ${brandId}`);

      const imageHtml = await this.embedsService.generateBrandImageHtml(
        Number(brandId),
      );

      if (!imageHtml) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getBrandImage',
          'Brand not found',
        );
      }

      res.setHeader('Content-Type', 'image/png');
      res.setHeader('Cache-Control', 'public, max-age=3600');
      return res.send(imageHtml);
    } catch (error) {
      this.logger.error(`Error generating brand image for ${brandId}:`, error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getBrandImage',
        error.message,
      );
    }
  }

  /**
   * Generate dynamic embed for brand sharing
   * URL: /embeds/brand/:brandId
   */
  @Get('/brand/:brandId')
  async getBrandEmbed(
    @Param('brandId') brandId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`Generating brand embed for brand ID: ${brandId}`);

      const embedHtml = await this.embedsService.generateBrandEmbed(
        Number(brandId),
      );

      if (!embedHtml) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getBrandEmbed',
          'Brand not found',
        );
      }

      res.setHeader('Content-Type', 'text/html');
      return res.send(embedHtml);
    } catch (error) {
      this.logger.error(`Error generating brand embed for ${brandId}:`, error);
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getBrandEmbed',
        error.message,
      );
    }
  }

  /**
   * Generate dynamic embed for leaderboard position sharing
   * URL: /embeds/leaderboard/:userId
   */
  @Get('/leaderboard/:userId')
  async getLeaderboardEmbed(
    @Param('userId') userId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`Generating leaderboard embed for user ID: ${userId}`);

      const embedHtml = await this.embedsService.generateLeaderboardEmbed(
        Number(userId),
      );

      if (!embedHtml) {
        return hasError(
          res,
          HttpStatus.NOT_FOUND,
          'getLeaderboardEmbed',
          'User not found',
        );
      }

      res.setHeader('Content-Type', 'text/html');
      return res.send(embedHtml);
    } catch (error) {
      this.logger.error(
        `Error generating leaderboard embed for ${userId}:`,
        error,
      );
      return hasError(
        res,
        HttpStatus.INTERNAL_SERVER_ERROR,
        'getLeaderboardEmbed',
        error.message,
      );
    }
  }

  /**
   * Debug endpoint to test if embeds controller is working
   */
  @Get('/debug/:voteId')
  async debugVote(
    @Param('voteId') voteId: string,
    @Res() res: Response,
  ): Promise<Response> {
    try {
      this.logger.log(`🔍 Debug: Checking vote ID: ${voteId}`);

      // Try to find the vote in the database
      const vote = await this.embedsService.debugFindVote(voteId);

      return res.json({
        success: true,
        voteId: voteId,
        found: !!vote,
        vote: vote || null,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      this.logger.error(`Debug error for ${voteId}:`, error);
      return res.status(500).json({
        success: false,
        error: error.message,
        voteId: voteId,
      });
    }
  }

  /**
   * Health check for embeds service
   */
  @Get('/health')
  async healthCheck(@Res() res: Response): Promise<Response> {
    return res.json({
      status: 'healthy',
      service: 'embeds',
      timestamp: new Date().toISOString(),
    });
  }
}
