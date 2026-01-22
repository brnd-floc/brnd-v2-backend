import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  createCanvas,
  loadImage,
  GlobalFonts,
  CanvasRenderingContext2D,
} from '@napi-rs/canvas';
import * as path from 'path';
import * as fs from 'fs';
import { v2 as cloudinary } from 'cloudinary';
import { Brand, User, UserBrandVotes } from '../../../models';

// TYPES
export interface BrandEntry {
  name: string;
  score: number;
  imageUrl: string;
  rank: 1 | 2 | 3;
}

export interface PodiumData {
  voteCost: number;
  username: string;
  level: number;
  transactionHash: string;
  userAvatarUrl: string;
  brands: BrandEntry[];
}

type CropRect = { sx: number; sy: number; sw: number; sh: number };

@Injectable()
export class PodiumService implements OnModuleInit {
  private readonly logger = new Logger(PodiumService.name);

  private readonly baseLayerPath = path.join(
    process.cwd(),
    'assets',
    'podium_base_layer.png',
  );

  private baseLayerCache: {
    image: Awaited<ReturnType<typeof loadImage>>;
    width: number;
    height: number;
  } | null = null;

  // ✅ Cached crop rectangle for the base image (removes transparent inset borders forever)
  private baseCrop: CropRect | null = null;

  private readonly CONFIG = {
    colors: {
      textWhite: '#FFFFFF',
      textGray: '#CCCCCC',
      background: '#000000',
    },
    fonts: {
      primary: 'Geist-Bold',
      fallback: 'Arial',
    },
    header: {
      userTextX: 1095,
      userTextY: 52,
      avatarX: 1110,
      avatarY: 22,
      avatarSize: 62,
    },
    slots: [
      { rank: 2, centerX: 344, y: 244, size: 220 },
      { rank: 1, centerX: 600, y: 164, size: 220 },
      { rank: 3, centerX: 856, y: 324, size: 220 },
    ],
  };

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {
    cloudinary.config({
      cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
      api_key: process.env.CLOUDINARY_API_KEY,
      api_secret: process.env.CLOUDINARY_API_SECRET,
    });
  }

  onModuleInit() {
    try {
      const fontPath = path.join(
        process.cwd(),
        'assets',
        'fonts',
        'Geist-Bold.ttf',
      );
      if (fs.existsSync(fontPath)) {
        GlobalFonts.registerFromPath(fontPath, 'Geist-Bold');
        this.logger.log('Custom font Geist-Bold registered successfully.');
      } else {
        this.logger.warn(`Custom font not found at ${fontPath}.`);
      }
    } catch (e) {
      this.logger.warn('Failed to register custom font', e as any);
    }
  }

  private async getBaseLayer() {
    if (this.baseLayerCache && this.baseCrop) return this.baseLayerCache;

    if (!fs.existsSync(this.baseLayerPath)) {
      throw new Error(`Base layer image not found at: ${this.baseLayerPath}`);
    }

    const image = await loadImage(this.baseLayerPath);
    this.baseLayerCache = { image, width: image.width, height: image.height };
    this.baseCrop = this.computeOpaqueBounds(image);

    this.logger.log(
      `Base layer loaded: ${image.width}x${image.height}, crop=${JSON.stringify(
        this.baseCrop,
      )}`,
    );

    return this.baseLayerCache;
  }

  /**
   * Finds the bounding box of all pixels with alpha > threshold.
   * This removes any transparent inset margins baked into the PNG.
   */
  private computeOpaqueBounds(img: any, alphaThreshold = 8): CropRect {
    const w = img.width as number;
    const h = img.height as number;

    const tmp = createCanvas(w, h);
    const tctx = tmp.getContext('2d');
    tctx.drawImage(img, 0, 0);

    const { data } = tctx.getImageData(0, 0, w, h);

    let minX = w,
      minY = h,
      maxX = -1,
      maxY = -1;

    for (let y = 0; y < h; y++) {
      const row = y * w * 4;
      for (let x = 0; x < w; x++) {
        const a = data[row + x * 4 + 3];
        if (a > alphaThreshold) {
          if (x < minX) minX = x;
          if (y < minY) minY = y;
          if (x > maxX) maxX = x;
          if (y > maxY) maxY = y;
        }
      }
    }

    // If the image is fully transparent (unexpected), fallback to full image
    if (maxX < 0 || maxY < 0) {
      return { sx: 0, sy: 0, sw: w, sh: h };
    }

    return {
      sx: minX,
      sy: minY,
      sw: maxX - minX + 1,
      sh: maxY - minY + 1,
    };
  }

  // async generatePodiumImageFromTxHash(
  //   transactionHash: string,
  // ): Promise<string> {
  //   try {
  //     const vote = await this.userBrandVotesRepository.findOne({
  //       where: { transactionHash },
  //       relations: ['user', 'brand1', 'brand2', 'brand3'],
  //     });
  //     if (!vote) throw new Error(`Vote not found: ${transactionHash}`);

  //     const podiumData = this.voteToPodiumData(vote);
  //     const imageBuffer = await this.generatePodiumImage(podiumData);

  //     const cloudinaryUrl = await this.uploadToCloudinary(
  //       imageBuffer,
  //       transactionHash,
  //     );

  //     await this.userBrandVotesRepository.update(
  //       { transactionHash },
  //       {
  //         podiumImageUrl: `https://miniappembeds-production.up.railway.app/podium/${transactionHash}`,
  //       },
  //     );

  //     return cloudinaryUrl;
  //   } catch (error) {
  //     this.logger.error('Error generating podium image:', error as any);
  //     throw error;
  //   }
  // }

  private async uploadToCloudinary(
    buffer: Buffer,
    transactionHash: string,
  ): Promise<string> {
    return new Promise((resolve, reject) => {
      cloudinary.uploader
        .upload_stream(
          {
            public_id: transactionHash,
            folder: 'podiums',
            format: 'png',
            overwrite: true,
            invalidate: true,
            resource_type: 'image',
          },
          (error, result) => {
            if (error) return reject(error);
            if (result?.secure_url) return resolve(result.secure_url);
            return reject(
              new Error('Cloudinary upload succeeded but no URL returned'),
            );
          },
        )
        .end(buffer);
    });
  }

  async getRecentVote(): Promise<UserBrandVotes> {
    const lastVotes = await this.userBrandVotesRepository.find({
      take: 55,
      order: { date: 'DESC' },
      relations: ['user', 'brand1', 'brand2', 'brand3'],
    });
    if (!lastVotes.length) throw new Error('No votes found');
    return lastVotes[Math.floor(Math.random() * lastVotes.length)];
  }

  private voteToPodiumData(vote: UserBrandVotes): PodiumData {
    return {
      username: vote.user.username,
      level: vote.user.brndPowerLevel,
      userAvatarUrl: vote.user.photoUrl,
      voteCost: vote.brndPaidWhenCreatingPodium,
      transactionHash: vote.transactionHash,
      brands: [
        {
          rank: 1,
          name: vote.brand1.name,
          score: vote.brand1.score,
          imageUrl: vote.brand1.imageUrl,
        },
        {
          rank: 2,
          name: vote.brand2.name,
          score: vote.brand2.score,
          imageUrl: vote.brand2.imageUrl,
        },
        {
          rank: 3,
          name: vote.brand3.name,
          score: vote.brand3.score,
          imageUrl: vote.brand3.imageUrl,
        },
      ],
    };
  }

  async generateSamplePodiumImage() {
    const vote = await this.getRecentVote();
    const podiumData = this.voteToPodiumData(vote);
    const buffer = await this.generatePodiumImage(podiumData);
    const cloudinaryUrl = await this.uploadToCloudinary(
      buffer,
      podiumData.transactionHash,
    );
    const localUpload = await this.uploadToLocal(
      buffer,
      podiumData.transactionHash,
    );
    return { buffer, cloudinaryUrl, localUpload };
  }

  private async uploadToLocal(
    buffer: Buffer,
    transactionHash: string,
  ): Promise<string> {
    const localPath = path.join(
      process.cwd(),
      'data',
      'podiums',
      `${transactionHash}.png`,
    );
    await fs.promises.mkdir(path.dirname(localPath), { recursive: true });
    await fs.promises.writeFile(localPath, buffer);
    return localPath;
  }

  /**
   * ✅ Forever-fix:
   * - Fill background (opaque)
   * - Draw the NON-TRANSPARENT cropped region of base image scaled to full canvas
   * => no inset transparent margin can appear in browsers/cloudinary/chat previews
   */
  async generatePodiumImage(data: PodiumData): Promise<Buffer> {
    console.log('GENERATING PODIUM IMAGE');
    try {
      const { image: baseImage, width, height } = await this.getBaseLayer();
      if (!this.baseCrop) throw new Error('Base crop not computed');

      const canvas = createCanvas(width, height);
      const ctx = canvas.getContext('2d');

      // Opaque background
      ctx.fillStyle = this.CONFIG.colors.background;
      ctx.fillRect(0, 0, width, height);

      // Draw only the opaque content of the base layer, stretched to full canvas
      const { sx, sy, sw, sh } = this.baseCrop;
      ctx.drawImage(baseImage, sx, sy, sw, sh, 0, 0, width, height);

      await this.drawUserHeader(ctx, data);

      for (const brand of data.brands) {
        await this.drawBrandSlot(ctx, brand, data.voteCost, height);
      }

      return canvas.encode('png');
    } catch (error) {
      this.logger.error('Error generating podium', error as any);
      throw error;
    }
  }

  private async drawUserHeader(
    ctx: CanvasRenderingContext2D,
    data: PodiumData,
  ) {
    const { header, colors, fonts } = this.CONFIG;

    ctx.textAlign = 'right';
    ctx.textBaseline = 'alphabetic';

    ctx.font = `bold 20px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textWhite;
    ctx.fillText(`by @${data.username}`, header.userTextX, header.userTextY);

    ctx.font = `16px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textGray;
    ctx.fillText(
      `LEVEL ${data.level}`,
      header.userTextX,
      header.userTextY + 20,
    );

    try {
      await this.drawRoundedImage(
        ctx,
        data.userAvatarUrl,
        header.avatarX,
        header.avatarY,
        header.avatarSize,
        header.avatarSize / 2,
      );
    } catch {
      // ignore avatar failures
    }
  }

  private async drawBrandSlot(
    ctx: CanvasRenderingContext2D,
    brand: BrandEntry,
    voteCost: number,
    canvasHeight: number,
  ) {
    const slot = this.CONFIG.slots.find((s) => s.rank === brand.rank);
    if (!slot) return;

    const x = slot.centerX - slot.size / 2;
    await this.drawRoundedImage(ctx, brand.imageUrl, x, slot.y, slot.size, 16);

    const { fonts, colors } = this.CONFIG;

    ctx.save();
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';

    ctx.font = `bold 25px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textWhite;
    ctx.fillText(brand.name, slot.centerX, canvasHeight - 85);

    const percentageMap: Record<1 | 2 | 3, number> = { 1: 0.6, 2: 0.3, 3: 0.1 };
    const voteAmount = Math.floor(voteCost * percentageMap[brand.rank]);

    ctx.font = `20px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillText(`${voteAmount} $BRND`, slot.centerX, canvasHeight - 55);

    ctx.restore();
  }

  private async drawRoundedImage(
    ctx: CanvasRenderingContext2D,
    url: string,
    x: number,
    y: number,
    size: number,
    radius: number,
  ) {
    try {
      const img = await loadImage(url);
      ctx.save();
      ctx.beginPath();
      ctx.roundRect(x, y, size, size, radius);
      ctx.clip();
      (ctx as any).drawImage(img, x, y, size, size);
      ctx.restore();
    } catch {
      // ignore image failures
    }
  }
}
