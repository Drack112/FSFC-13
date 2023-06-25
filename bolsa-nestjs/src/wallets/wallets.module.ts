import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';

import { WalletsService } from './wallets.service';
import { WalletsController } from './wallets.controller';
import { WalletAssetsService } from './wallet-assets/wallet-assets.service';
import { WalletAssetsController } from './wallet-assets/wallet-assets.controller';
import {
  WalletAsset,
  WalletAssetSchema,
} from './wallet-assets/wallet-asset.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: WalletAsset.name, schema: WalletAssetSchema },
    ]),
  ],
  controllers: [WalletsController, WalletAssetsController],
  providers: [WalletsService, WalletAssetsService],
  exports: [WalletsService, WalletAssetsService],
})
export class WalletsModule {}
