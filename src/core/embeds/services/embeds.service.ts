// src/core/embeds/services/embeds.service.ts

import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { UserBrandVotes, User, Brand } from '../../../models';
import { getConfig } from '../../../security/config';

import { EmbedData } from './embeds.types';

@Injectable()
export class EmbedsService {
  private readonly logger = new Logger(EmbedsService.name);
  private readonly config = getConfig();
  private readonly fallbackImageUrl = 'https://brnd.land/image.png';

  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly votesRepository: Repository<UserBrandVotes>,

    @InjectRepository(User)
    private readonly userRepository: Repository<User>,

    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
  ) {}

  async generatePodiumEmbedByTransactionHash(
    transactionHash: string,
  ): Promise<string | null> {
    try {
      const vote = await this.votesRepository.findOne({
        where: { transactionHash: transactionHash },
        relations: ['user', 'brand1', 'brand2', 'brand3'],
      });

      if (!vote) {
        this.logger.warn(`Vote not found: ${transactionHash}`);
        // Return a default embed instead of null
        return this.generateDefaultPodiumEmbed(transactionHash);
      }

      const primaryImageUrl =
        vote.podiumImageUrl ||
        `https://res.cloudinary.com/dkjfbaniw/image/upload/podiums/${transactionHash}.png`;
      const imageUrl = await this.ensureImageUrl(primaryImageUrl);

      const embedData: EmbedData = {
        title: `BRND`,
        description: `${vote.user.username}'s BRND Podium`,
        imageUrl,
        targetUrl: `https://brnd.land`,
      };
      return this.generateEmbedHtml(embedData, 'podium');
    } catch (error) {
      this.logger.error(
        `Error generating podium embed for ${transactionHash}:`,
        error,
      );
      // Return default embed on error instead of null
      return this.generateDefaultPodiumEmbed(transactionHash);
    }
  }

  private async ensureImageUrl(url: string): Promise<string> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);

    try {
      const res = await fetch(url, {
        method: 'HEAD',
        signal: controller.signal,
      });
      clearTimeout(timeout);

      if (res.ok) {
        const contentType = res.headers.get('content-type') || '';
        if (contentType.startsWith('image/')) {
          return url;
        }
      } else {
        this.logger.warn(
          `Image check failed with status ${res.status} for ${url}`,
        );
      }
    } catch (error) {
      clearTimeout(timeout);
      this.logger.warn(`Image check failed for ${url}`, error);
    }

    return this.fallbackImageUrl;
  }

  /**
   * Generate HTML that renders as an image for podium
   */
  async generatePodiumImageHtml(voteId: string): Promise<string | null> {
    try {
      // Get the vote with all related data
      const vote = await this.votesRepository.findOne({
        where: { transactionHash: voteId },
        relations: ['user', 'brand1', 'brand2', 'brand3'],
      });

      if (!vote) {
        this.logger.warn(`Vote not found for image: ${voteId}`);
        return null;
      }

      return this.generatePodiumImageTemplate(vote);
    } catch (error) {
      this.logger.error(
        `Error generating podium image HTML for ${voteId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Generate brand embed with proper image URL
   */
  async generateBrandEmbed(brandId: number): Promise<string | null> {
    try {
      const brand = await this.brandRepository.findOne({
        where: { id: brandId },
        relations: ['category'],
      });

      if (!brand) {
        this.logger.warn(`Brand not found: ${brandId}`);
        return null;
      }

      const embedData: EmbedData = {
        title: `${brand.name} on BRND`,
        description: `The page for ${brand.name} on BRND`,
        imageUrl: 'https://brnd.land/image.png',
        targetUrl: `https://brnd.land/brand/${brandId}`,
      };

      return this.generateEmbedHtml(embedData, 'brand');
    } catch (error) {
      this.logger.error(`Error generating brand embed for ${brandId}:`, error);
      return null;
    }
  }

  /**
   * Debug method to check if vote exists
   */
  async debugFindVote(voteId: string): Promise<any> {
    try {
      this.logger.log(`🔍 Looking for vote: ${voteId}`);

      const vote = await this.votesRepository.findOne({
        where: { transactionHash: voteId },
        relations: ['user', 'brand1', 'brand2', 'brand3'],
      });

      if (vote) {
        this.logger.log(`✅ Vote found: ${vote.user?.username}'s podium`);
        return {
          transactionHash: vote.transactionHash,
          date: vote.date,
          user: vote.user?.username,
          brand1: vote.brand1?.name,
          brand2: vote.brand2?.name,
          brand3: vote.brand3?.name,
        };
      } else {
        this.logger.warn(`❌ Vote not found: ${voteId}`);
        return null;
      }
    } catch (error) {
      this.logger.error(`Debug error:`, error);
      throw error;
    }
  }

  /**
   * Generate brand image HTML
   */
  async generateBrandImageHtml(brandId: number): Promise<string | null> {
    try {
      const brand = await this.brandRepository.findOne({
        where: { id: brandId },
        relations: ['category'],
      });

      if (!brand) {
        this.logger.warn(`Brand not found for image: ${brandId}`);
        return null;
      }

      return this.generateBrandImageTemplate(brand);
    } catch (error) {
      this.logger.error(
        `Error generating brand image HTML for ${brandId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Generate leaderboard embed with proper image URL
   */
  async generateLeaderboardEmbed(userId: number): Promise<string | null> {
    try {
      const user = await this.userRepository.findOne({
        where: { id: userId },
      });

      if (!user) {
        this.logger.warn(`User not found: ${userId}`);
        return null;
      }

      // Get user's rank (simplified - you might want to use your existing leaderboard logic)
      const rank = await this.getUserRank(userId);

      const embedData: EmbedData = {
        title: `${user.username} on BRND Leaderboard`,
        description: `Rank #${rank} with ${user.points} points | Join the competition and vote for your favorite brands!`,
        imageUrl:
          'https://github.com/jpfraneto/images/blob/main/dynamic.png?raw=true',
        targetUrl: `https://brnd.land/leaderboard`,
      };

      return this.generateEmbedHtml(embedData, 'leaderboard');
    } catch (error) {
      this.logger.error(
        `Error generating leaderboard embed for ${userId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Generate leaderboard image HTML
   */
  async generateLeaderboardImageHtml(userId: number): Promise<string | null> {
    try {
      const user = await this.userRepository.findOne({
        where: { id: userId },
      });

      if (!user) {
        this.logger.warn(`User not found for leaderboard image: ${userId}`);
        return null;
      }

      const rank = await this.getUserRank(userId);
      return this.generateLeaderboardImageTemplate(user, rank);
    } catch (error) {
      this.logger.error(
        `Error generating leaderboard image HTML for ${userId}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Generate the actual HTML with Farcaster frame metadata
   */
  private generateEmbedHtml(
    embedData: EmbedData,
    type: string,
    title: string = 'START BRND',
  ): string {
    const timestamp = Date.now();
    // Ensure we handle existing query params in the imageUrl
    const dynamicImageUrl = `${embedData.imageUrl}${embedData.imageUrl.includes('?') ? '&' : '?'}t=${timestamp}`;

    const frameData = JSON.stringify({
      version: '1', // Corrected version
      imageUrl: dynamicImageUrl, // Using the cache-busted URL
      button: {
        title: title,
        action: {
          type: 'launch_frame',
          url: embedData.targetUrl,
          name: 'BRND',
          splashImageUrl: 'https://brnd.land/splash.png',
          splashBackgroundColor: '#000000',
        },
      },
    });

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    
    <meta name="fc:miniapp" content='${frameData}'/>
    <meta name="fc:frame" content='${frameData}'/>
    
    <title>${embedData.title}</title>
    <meta name="description" content="${embedData.description}">
    
    <meta property="og:title" content="${embedData.title}">
    <meta property="og:description" content="${embedData.description}">
    <meta property="og:image" content="${dynamicImageUrl}">
    <meta property="og:url" content="${embedData.targetUrl}">
    <meta property="og:type" content="website">
    <meta property="og:site_name" content="BRND">
    
    <meta name="twitter:card" content="summary_large_image">
    <meta name="twitter:title" content="${embedData.title}">
    <meta name="twitter:description" content="${embedData.description}">
    <meta name="twitter:image" content="${dynamicImageUrl}">
    <meta property="twitter:domain" content="brnd.land">
    <meta property="twitter:url" content="${embedData.targetUrl}">
    
    <meta name="author" content="BRND Team">
    <meta name="keywords" content="BRND, Farcaster, brands, voting, leaderboard, ${type}">
</head>
<body>
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 40px 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; text-align: center; min-height: 100vh; display: flex; flex-direction: column; justify-content: center; align-items: center;">
        <div style="max-width: 600px; background: rgba(255, 255, 255, 0.1); padding: 40px; border-radius: 20px; backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.2);">
            <h1 style="font-size: 2.5rem; margin-bottom: 20px; font-weight: 700;">BRND</h1>
            <p style="font-size: 1.2rem; margin-bottom: 30px; opacity: 0.9; line-height: 1.6;">${embedData.description}</p>
            <a href="${embedData.targetUrl}" style="background: rgba(255, 255, 255, 0.2); padding: 15px 30px; border-radius: 50px; border: 2px solid rgba(255, 255, 255, 0.3); font-weight: 600; text-decoration: none; color: white; display: inline-block;">Open in BRND</a>
        </div>
    </div>
</body>
</html>`;
  }

  /**
   * Generate podium image template (1:1.91 aspect ratio - 763px × 400px)
   */
  private generatePodiumImageTemplate(vote: any): string {
    const timestamp = new Date().toISOString();

    return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=763, height=400">
    <style>
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: white;
            width: 763px;
            height: 400px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            position: relative;
            overflow: hidden;
            box-sizing: border-box;
        }
        .container {
            text-align: center;
            padding: 30px;
            position: relative;
            z-index: 2;
            width: 100%;
        }
        .title {
            font-size: 42px;
            font-weight: 900;
            margin-bottom: 15px;
            background: linear-gradient(45deg, #fff, #a8d5ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 2px 10px rgba(0,0,0,0.3);
            line-height: 1;
        }
        .subtitle {
            font-size: 16px;
            margin-bottom: 25px;
            opacity: 0.9;
        }
        .podium {
            display: flex;
            justify-content: center;
            align-items: flex-end;
            gap: 15px;
            margin: 20px 0;
        }
        .place {
            background: rgba(255, 255, 255, 0.15);
            border-radius: 10px;
            padding: 12px 8px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.25);
            min-width: 110px;
            max-width: 140px;
        }
        .place-1 { 
            transform: translateY(-8px); 
            background: rgba(255, 215, 0, 0.25);
            border-color: rgba(255, 215, 0, 0.4);
        }
        .place-2 { 
            transform: translateY(0px); 
            background: rgba(192, 192, 192, 0.25);
            border-color: rgba(192, 192, 192, 0.4);
        }
        .place-3 { 
            transform: translateY(3px); 
            background: rgba(205, 127, 50, 0.25);
            border-color: rgba(205, 127, 50, 0.4);
        }
        .emoji { 
            font-size: 20px; 
            margin-bottom: 6px;
            display: block;
        }
        .brand-name { 
            font-size: 13px; 
            font-weight: 600; 
            margin-bottom: 3px;
            line-height: 1.2;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }
        .score { 
            font-size: 11px; 
            opacity: 0.8; 
        }
        .timestamp {
            position: absolute;
            bottom: 8px;
            right: 12px;
            font-size: 9px;
            opacity: 0.6;
        }
        .decoration {
            position: absolute;
            top: -50px;
            right: -50px;
            width: 150px;
            height: 150px;
            background: radial-gradient(circle, rgba(255,255,255,0.08) 0%, transparent 70%);
            border-radius: 50%;
        }
        .decoration2 {
            position: absolute;
            bottom: -30px;
            left: -30px;
            width: 120px;
            height: 120px;
            background: radial-gradient(circle, rgba(102, 126, 234, 0.15) 0%, transparent 70%);
            border-radius: 50%;
        }
    </style>
</head>
<body>
    <div class="decoration"></div>
    <div class="decoration2"></div>
    <div class="container">
        <div class="title">BRND</div>
        <div class="subtitle">${vote.user.username}'s Podium</div>
        <div class="podium">
            <div class="place place-2">
                <div class="emoji">🥈</div>
                <div class="brand-name">${vote.brand2.name}</div>
                <div class="score">30 pts</div>
            </div>
            <div class="place place-1">
                <div class="emoji">🥇</div>
                <div class="brand-name">${vote.brand1.name}</div>
                <div class="score">60 pts</div>
            </div>
            <div class="place place-3">
                <div class="emoji">🥉</div>
                <div class="brand-name">${vote.brand3.name}</div>
                <div class="score">10 pts</div>
            </div>
        </div>
    </div>
    <div class="timestamp">Generated: ${timestamp}</div>
</body>
</html>`;
  }

  /**
   * Generate brand image template
   */
  private generateBrandImageTemplate(brand: any): string {
    const timestamp = new Date().toISOString();

    return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=763, height=400">
    <style>
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            width: 763px;
            height: 400px;
            display: flex;
            justify-content: center;
            align-items: center;
            text-align: center;
            position: relative;
            overflow: hidden;
            box-sizing: border-box;
        }
        .content { 
            padding: 40px;
            max-width: 600px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        h1 { 
            font-size: 48px; 
            margin-bottom: 20px; 
            font-weight: 700;
            background: linear-gradient(45deg, #fff, #a8d5ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            line-height: 1;
        }
        .brand-name {
            font-size: 28px;
            margin-bottom: 15px;
            font-weight: 600;
            line-height: 1.2;
        }
        .description { 
            font-size: 16px; 
            line-height: 1.4; 
            opacity: 0.9;
            margin-bottom: 10px;
        }
        .score {
            font-size: 20px;
            font-weight: 600;
            color: #a8d5ff;
        }
        .timestamp { 
            position: absolute; 
            bottom: 10px; 
            right: 10px; 
            font-size: 10px; 
            opacity: 0.7; 
        }
    </style>
</head>
<body>
    <div class="content">
        <h1>BRND</h1>
        <div class="brand-name">${brand.name}</div>
        <div class="description">${brand.category?.name || 'Brand'}</div>
        <div class="score">${brand.score} points</div>
    </div>
    <div class="timestamp">${timestamp}</div>
</body>
</html>`;
  }

  /**
   * Generate leaderboard image template
   */
  private generateLeaderboardImageTemplate(user: any, rank: number): string {
    const timestamp = new Date().toISOString();

    return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=763, height=400">
    <style>
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: white;
            width: 763px;
            height: 400px;
            display: flex;
            justify-content: center;
            align-items: center;
            text-align: center;
            position: relative;
            overflow: hidden;
            box-sizing: border-box;
        }
        .content { 
            padding: 40px;
            max-width: 600px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        h1 { 
            font-size: 48px; 
            margin-bottom: 20px; 
            font-weight: 700;
            background: linear-gradient(45deg, #fff, #a8d5ff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            line-height: 1;
        }
        .username {
            font-size: 28px;
            margin-bottom: 15px;
            font-weight: 600;
            line-height: 1.2;
        }
        .rank {
            font-size: 36px;
            font-weight: 700;
            color: #ffd700;
            margin-bottom: 10px;
        }
        .points {
            font-size: 20px;
            opacity: 0.9;
            margin-bottom: 10px;
        }
        .subtitle {
            font-size: 14px;
            opacity: 0.7;
        }
        .timestamp { 
            position: absolute; 
            bottom: 10px; 
            right: 10px; 
            font-size: 10px; 
            opacity: 0.7; 
        }
        .decoration {
            position: absolute;
            top: -50px;
            right: -50px;
            width: 150px;
            height: 150px;
            background: radial-gradient(circle, rgba(255,215,0,0.1) 0%, transparent 70%);
            border-radius: 50%;
        }
    </style>
</head>
<body>
    <div class="decoration"></div>
    <div class="content">
        <h1>BRND</h1>
        <div class="username">${user.username}</div>
        <div class="rank">#${rank}</div>
        <div class="points">${user.points} points</div>
        <div class="subtitle">BRND Leaderboard</div>
    </div>
    <div class="timestamp">${timestamp}</div>
</body>
</html>`;
  }

  /**
   * Convert date to unix timestamp for vote URL compatibility
   */
  private getUnixDateFromVote(createdAt: Date): number {
    return Math.floor(createdAt.getTime() / 1000);
  }

  /**
   * Get user's rank in leaderboard (simplified version)
   */
  private async getUserRank(userId: number): Promise<number> {
    try {
      const usersWithHigherScores = await this.userRepository
        .createQueryBuilder('user')
        .where('user.points > (SELECT points FROM users WHERE id = :userId)', {
          userId,
        })
        .getCount();

      return usersWithHigherScores + 1;
    } catch (error) {
      this.logger.error('Error calculating user rank:', error);
      return 0;
    }
  }

  /**
   * Generate default embed when vote is not found
   */
  private generateDefaultPodiumEmbed(transactionHash: string): string {
    const embedData: EmbedData = {
      title: `BRND`,
      description: `BRND Podium`,
      imageUrl: 'https://brnd.land/image.png',
      targetUrl: `https://brnd.land`,
    };
    return this.generateEmbedHtml(embedData, 'podium');
  }
}
