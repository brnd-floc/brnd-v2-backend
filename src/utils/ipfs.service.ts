import { Injectable, Logger } from '@nestjs/common';
import { getConfig } from '../security/config';

@Injectable()
export class IpfsService {
  private readonly logger = new Logger(IpfsService.name);
  private readonly pinataApiKey = process.env.PINATA_API_KEY;
  private readonly pinataSecretKey = process.env.PINATA_SECRET_KEY;
  private readonly pinataJwt = process.env.PINATA_JWT;
  private readonly usePinata = !!(this.pinataApiKey || this.pinataJwt);

  /**
   * Uploads JSON metadata to IPFS
   * Uses Pinata if credentials are available, otherwise uses a public IPFS gateway
   */
  async uploadJsonToIpfs(metadata: object): Promise<string> {
    try {
      if (this.usePinata) {
        return await this.uploadToPinata(metadata);
      } else {
        // Fallback to public IPFS gateway (like web3.storage or nft.storage)
        // For now, we'll require Pinata credentials
        throw new Error(
          'IPFS upload requires PINATA_API_KEY and PINATA_SECRET_KEY or PINATA_JWT environment variables',
        );
      }
    } catch (error) {
      this.logger.error('Failed to upload to IPFS:', error);
      throw new Error(`IPFS upload failed: ${error.message}`);
    }
  }

  /**
   * Uploads JSON to Pinata IPFS using JSON endpoint (simpler than file upload)
   */
  private async uploadToPinata(metadata: object): Promise<string> {
    try {
      let headers: HeadersInit;
      if (this.pinataJwt) {
        // Use JWT authentication (preferred)
        headers = {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${this.pinataJwt}`,
        };
      } else if (this.pinataApiKey && this.pinataSecretKey) {
        // Use API key authentication
        headers = {
          'Content-Type': 'application/json',
          pinata_api_key: this.pinataApiKey,
          pinata_secret_api_key: this.pinataSecretKey,
        };
      } else {
        throw new Error('Pinata credentials not configured');
      }

      const pinataBody = {
        pinataContent: metadata,
        pinataMetadata: {
          name: 'Brand Metadata',
        },
        pinataOptions: {
          cidVersion: 1,
        },
      };

      const response = await fetch('https://api.pinata.cloud/pinning/pinJSONToIPFS', {
        method: 'POST',
        headers,
        body: JSON.stringify(pinataBody),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Pinata API error: ${response.status} - ${errorText}`);
      }

      const result = await response.json();
      const ipfsHash = result.IpfsHash;

      if (!ipfsHash) {
        throw new Error('Pinata response missing IpfsHash');
      }

      this.logger.log(`✅ Uploaded to IPFS: ${ipfsHash}`);
      return ipfsHash;
    } catch (error) {
      this.logger.error('Pinata upload error:', error);
      throw error;
    }
  }
}

