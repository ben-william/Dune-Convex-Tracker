-- get all dao wallet holdings grouped by day
-- get all cvx transfers where to or from match a dao (label daos here)
--- filter this table by to/from contract addys, assign stake, unstake, lock, unlocks by dao with +/-
-- sum holdings, stake/unstake, lock/unlocks partition by dao order by date
-- union with full date series
WITH
  CVX_holdings AS (
    SELECT
      day,
      SUM(amount) as "Wallet CVX",
      wallet_address,
      CASE
        WHEN wallet_address IN (
          '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
          '\x49ee75278820f409ecd67063d8d717b38d66bd71',
          '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27'
        ) THEN 'Frax'
        WHEN wallet_address IN (
          '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
          '\x898111d1F4eB55025D0036568212425EE2274082'
        ) THEN 'Badger'
        WHEN wallet_address IN (
          '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
          '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
          '\x2d643Df5De4e9Ba063760d475BEAa62821c71681'
        ) THEN 'Olympus'
        WHEN wallet_address IN (
          '\x246c00f93001c344038A0d9Cf012754b539c1014',
          '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9'
        ) THEN 'Alchemix'
        WHEN wallet_address IN (
          '\xdc71417E173955d100aF4fc9673493Fff244514C',
          '\x597f540bb63381ffa267027d2d479984825057a8'
        ) THEN 'Mochi Inu'
        WHEN wallet_address IN (
          '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
          '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5'
        ) THEN 'Origin'
        WHEN wallet_address IN ('\x2CA74be68f0A0e053F030D143C1376806BaBEdc9') THEN 'Jarvis'
        WHEN wallet_address IN ('\x9a67F1940164d0318612b497E8e6038f902a00a4') THEN 'KeeperDAO'
        WHEN wallet_address IN ('\x67905d3e4fec0c85dce68195f66dc8eb32f59179') THEN 'mStable'
        WHEN wallet_address IN ('\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341') THEN 'Wonderland'
        WHEN wallet_address IN ('\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b') THEN 'Bent Finance'
        WHEN wallet_address IN (
          '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
          '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b'
        ) THEN 'Redacted'
        WHEN wallet_address IN ('\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83') THEN 'K3PR'
        WHEN wallet_address IN (
          '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
          '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682'
        ) THEN 'Terra'
        WHEN wallet_address IN ('\x42e61987a5cba002880b3cc5c800952a5804a1c5') THEN 'SquidDAO'
        WHEN wallet_address IN (
          '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
          '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb'
        ) THEN 'Congruent'
        WHEN wallet_address IN ('\x5c8898f8e0f9468d4a677887bc03ee2659321012') THEN 'TempleDAO'
        WHEN wallet_address IN (
          '\xc13d06194e149ea53f6c823d9446b100eed37042',
          '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e'
        ) THEN 'Reverse Protocol'
        WHEN wallet_address IN ('\x726d658d23834a2289126267c331b3f0aa03c5ad') THEN 'Diamond DAO'
        WHEN wallet_address IN ('\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f') THEN 'Threshold Network'
        WHEN wallet_address IN ('\x26f539A0fE189A7f228D7982BF10Bc294FA9070c') THEN 'DFX Finance'
        WHEN wallet_address IN ('\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07') THEN 'Silo Finance'
        WHEN wallet_address IN (
          '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
          '\x8b4334d4812c530574bd4f2763fcd22de94a969b'
        ) THEN 'Tokemak'
        WHEN wallet_address IN ('\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f') THEN 'Jones DAO'
        WHEN wallet_address IN (
          '\x51c2cef9efa48e08557a361b52db34061c025a1b',
          '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277'
        ) THEN 'JPEGd'
        WHEN wallet_address IN ('\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6') THEN 'Umami Finance'
        ELSE wallet_address:: text
      END AS "DAO"
      
    FROM erc20."view_token_balances_daily"
    
    WHERE token_address = '\x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B' -- CVX token
    
      AND wallet_address IN (
        '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5',
        '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
        '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b',
        '\x246c00f93001c344038A0d9Cf012754b539c1014',
        '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9',
        '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
        '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682',
        '\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b',
        '\x9a67F1940164d0318612b497E8e6038f902a00a4',
        '\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341',
        '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
        '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
        '\xdc71417E173955d100aF4fc9673493Fff244514C',
        '\x42e61987a5cba002880b3cc5c800952a5804a1c5',
        '\x2CA74be68f0A0e053F030D143C1376806BaBEdc9',
        '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
        '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
        '\x5c8898f8e0f9468d4a677887bc03ee2659321012',
        '\x726d658d23834a2289126267c331b3f0aa03c5ad',
        '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
        '\xc13d06194e149ea53f6c823d9446b100eed37042',
        '\x49ee75278820f409ecd67063d8d717b38d66bd71',
        '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb',
        '\x26f539A0fE189A7f228D7982BF10Bc294FA9070c',
        '\x597f540bb63381ffa267027d2d479984825057a8',
        '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
        '\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07',
        '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27',
        '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e',
        '\x898111d1F4eB55025D0036568212425EE2274082',
        '\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83',
        '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
        '\x8b4334d4812c530574bd4f2763fcd22de94a969b',
        '\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f',
        '\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6',
        '\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f',
        '\x2d643Df5De4e9Ba063760d475BEAa62821c71681',
        '\x51c2cef9efa48e08557a361b52db34061c025a1b',
        '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277',
        '\x67905d3e4fec0c85dce68195f66dc8eb32f59179')
    GROUP BY 1,3
),
all_cvx_xfers AS (
SELECT
  DATE_TRUNC('day', evt_block_time) AS "date",
  value / 1e18 as amount,
  "to",
  "from",
  evt_tx_hash
FROM erc20."ERC20_evt_Transfer"
WHERE
    "to" IN (
        '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5',
        '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
        '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b',
        '\x246c00f93001c344038A0d9Cf012754b539c1014',
        '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9',
        '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
        '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682',
        '\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b',
        '\x9a67F1940164d0318612b497E8e6038f902a00a4',
        '\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341',
        '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
        '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
        '\xdc71417E173955d100aF4fc9673493Fff244514C',
        '\x42e61987a5cba002880b3cc5c800952a5804a1c5',
        '\x2CA74be68f0A0e053F030D143C1376806BaBEdc9',
        '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
        '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
        '\x5c8898f8e0f9468d4a677887bc03ee2659321012',
        '\x726d658d23834a2289126267c331b3f0aa03c5ad',
        '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
        '\xc13d06194e149ea53f6c823d9446b100eed37042',
        '\x49ee75278820f409ecd67063d8d717b38d66bd71',
        '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb',
        '\x26f539A0fE189A7f228D7982BF10Bc294FA9070c',
        '\x597f540bb63381ffa267027d2d479984825057a8',
        '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
        '\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07',
        '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27',
        '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e',
        '\x898111d1F4eB55025D0036568212425EE2274082',
        '\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83',
        '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
        '\x8b4334d4812c530574bd4f2763fcd22de94a969b',
        '\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f',
        '\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6',
        '\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f',
        '\x2d643Df5De4e9Ba063760d475BEAa62821c71681',
        '\x51c2cef9efa48e08557a361b52db34061c025a1b',
        '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277',
        '\x67905d3e4fec0c85dce68195f66dc8eb32f59179')
    OR "from" IN (
        '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5',
        '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
        '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b',
        '\x246c00f93001c344038A0d9Cf012754b539c1014',
        '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9',
        '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
        '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682',
        '\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b',
        '\x9a67F1940164d0318612b497E8e6038f902a00a4',
        '\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341',
        '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
        '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
        '\xdc71417E173955d100aF4fc9673493Fff244514C',
        '\x42e61987a5cba002880b3cc5c800952a5804a1c5',
        '\x2CA74be68f0A0e053F030D143C1376806BaBEdc9',
        '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
        '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
        '\x5c8898f8e0f9468d4a677887bc03ee2659321012',
        '\x726d658d23834a2289126267c331b3f0aa03c5ad',
        '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
        '\xc13d06194e149ea53f6c823d9446b100eed37042',
        '\x49ee75278820f409ecd67063d8d717b38d66bd71',
        '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb',
        '\x26f539A0fE189A7f228D7982BF10Bc294FA9070c',
        '\x597f540bb63381ffa267027d2d479984825057a8',
        '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
        '\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07',
        '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27',
        '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e',
        '\x898111d1F4eB55025D0036568212425EE2274082',
        '\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83',
        '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
        '\x8b4334d4812c530574bd4f2763fcd22de94a969b',
        '\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f',
        '\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6',
        '\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f',
        '\x2d643Df5De4e9Ba063760d475BEAa62821c71681',
        '\x51c2cef9efa48e08557a361b52db34061c025a1b',
        '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277',
        '\x67905d3e4fec0c85dce68195f66dc8eb32f59179')
  AND "contract_address" = '\x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b' -- CVX Token
  ),
  cvx_xfer_actions AS (
  SELECT
    "date",
    amount,
    CASE
        WHEN "to" = '\xcf50b810e57ac33b91dcf525c6ddd9881b139332' THEN 'Stake' -- CVX Stake Address
        WHEN "from" = '\xcf50b810e57ac33b91dcf525c6ddd9881b139332' THEN 'Unstake' -- CVX Stake Address
        WHEN ("to" = '\xd18140b4b819b895a3dba5442f959fa44994af50' -- Original CVX Locker Address
                OR "to" = '\x72a19342e8f1838460ebfccef09f6585e32db86e')-- New CVX Locker Address 
                THEN 'Lock'
        WHEN ("from" = '\xd18140b4b819b895a3dba5442f959fa44994af50' -- Original CVX Locker Address
                OR "from" = '\x72a19342e8f1838460ebfccef09f6585e32db86e')-- New CVX Locker Address
                THEN 'Unlock'
        ELSE 'Other'
        END AS action,
    CASE
        WHEN ("to" IN (
          '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
          '\x49ee75278820f409ecd67063d8d717b38d66bd71',
          '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27'
            ) OR "from" IN (
              '\x7038C406e7e2C9F81571557190d26704bB39B8f3',
              '\x49ee75278820f409ecd67063d8d717b38d66bd71',
              '\xB1748C79709f4Ba2Dd82834B8c82D4a505003f27'
            )) THEN 'Frax'
        WHEN ("to"  IN (
          '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
          '\x898111d1F4eB55025D0036568212425EE2274082'
            ) OR "from" IN (
              '\x3ff634ce65cdb8cc0d569d6d1697c41aa666cea9',
              '\x898111d1F4eB55025D0036568212425EE2274082'
            )) THEN 'Badger'
        WHEN ("to"  IN (
          '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
          '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
          '\x2d643Df5De4e9Ba063760d475BEAa62821c71681'
            ) OR "from" IN (
              '\xdfc95aaf0a107daae2b350458ded4b7906e7f728',
              '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8',
              '\x2d643Df5De4e9Ba063760d475BEAa62821c71681'
            )) THEN 'Olympus'
        WHEN ("to"  IN (
          '\x246c00f93001c344038A0d9Cf012754b539c1014',
          '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9'
            ) OR "from" IN (
            '\x246c00f93001c344038A0d9Cf012754b539c1014',
            '\x9e2b6378ee8ad2A4A95Fe481d63CAba8FB0EBBF9'          
            )) THEN 'Alchemix'
        WHEN ("to"  IN (
          '\xdc71417E173955d100aF4fc9673493Fff244514C',
          '\x597f540bb63381ffa267027d2d479984825057a8'
            ) OR "from" IN (
            '\xdc71417E173955d100aF4fc9673493Fff244514C',
            '\x597f540bb63381ffa267027d2d479984825057a8'  
            ))THEN 'Mochi Inu'
        WHEN ("to"  IN (
          '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
          '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5'
            ) OR "from" IN (
            '\x70fCE97d671E81080CA3ab4cc7A59aAc2E117137',
            '\x449E0B5564e0d141b3bc3829E74fFA0Ea8C08ad5'  
            ))THEN 'Origin'
        WHEN ("to"  IN ('\x2CA74be68f0A0e053F030D143C1376806BaBEdc9') OR "from" IN ('\x2CA74be68f0A0e053F030D143C1376806BaBEdc9')) THEN 'Jarvis'
        WHEN ("to"  IN ('\x9a67F1940164d0318612b497E8e6038f902a00a4') OR "from" IN ('\x9a67F1940164d0318612b497E8e6038f902a00a4')) THEN 'KeeperDAO'
        WHEN ("to"  IN ('\x67905d3e4fec0c85dce68195f66dc8eb32f59179') OR "from" IN ('\x67905d3e4fec0c85dce68195f66dc8eb32f59179')) THEN 'mStable'
        WHEN ("to"  IN ('\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341') OR "from" IN ('\x355D72Fb52AD4591B2066E43e89A7A38CF5cb341')) THEN 'Wonderland'
        WHEN ("to"  IN ('\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b') OR "from" IN ('\xe001452bec9e7ac34ca4ecac56e7e95ed9c9aa3b')) THEN 'Bent Finance'
        WHEN ("to"  IN (
         '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
         '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b'
        ) OR "from" IN (
        '\xa52fd396891e7a74b641a2cb1a6999fcf56b077e',
        '\x086c98855df3c78c6b481b6e1d47bef42e9ac36b')) THEN 'Redacted'
        WHEN ("to"  IN ('\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83') OR "from" IN ('\x0D5Dc686d0a2ABBfDaFDFb4D0533E886517d4E83')) THEN 'K3PR'
        WHEN ("to"  IN (
          '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
          '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682'
        ) OR "from" IN (
            '\x9538D438d506Fc426dB37fb83daC2a0752A02757',
            '\xbEC5E1AD5422e52821735b59b39Dc03810aAe682')) THEN 'Terra'
        WHEN ("to"  IN ('\x42e61987a5cba002880b3cc5c800952a5804a1c5') OR "from" IN ('\x42e61987a5cba002880b3cc5c800952a5804a1c5')) THEN 'SquidDAO'
        WHEN ("to"  IN (
          '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
          '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb'
        ) OR "from" IN (
            '\x4a6bf7737b54195BFB72030d8F2bf7Cf2b466dC3',
            '\x73141d278a9c71d2ef2a0b83565e9d5728fa15cb')) THEN 'Congruent'
        WHEN ("to"  IN ('\x5c8898f8e0f9468d4a677887bc03ee2659321012') OR "from" IN ('\x5c8898f8e0f9468d4a677887bc03ee2659321012')) THEN 'TempleDAO'
        WHEN ("to"  IN (
          '\xc13d06194e149ea53f6c823d9446b100eed37042',
          '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e'
        ) OR "from" IN (
            '\xc13d06194e149ea53f6c823d9446b100eed37042',
            '\x5fBA3e7EeEB50A4Dc3328e2f974e0D608b38913e')) THEN 'Reverse Protocol'
        WHEN ("to"  IN ('\x726d658d23834a2289126267c331b3f0aa03c5ad') OR "from" IN ('\x726d658d23834a2289126267c331b3f0aa03c5ad')) THEN 'Diamond DAO'
        WHEN ("to"  IN ('\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f') OR "from" IN ('\x9f6e831c8f8939dc0c830c6e492e7cef4f9c2f5f')) THEN 'Threshold Network'
        WHEN ("to"  IN ('\x26f539A0fE189A7f228D7982BF10Bc294FA9070c') OR "from" IN ('\x26f539A0fE189A7f228D7982BF10Bc294FA9070c')) THEN 'DFX Finance'
        WHEN ("to"  IN ('\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07') OR "from" IN ('\xDfF2aeA378e41632E45306A6dE26A7E0Fd93AB07')) THEN 'Silo Finance'
        WHEN ("to"  IN (
          '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
          '\x8b4334d4812c530574bd4f2763fcd22de94a969b'
        ) OR "from" IN (
            '\xa86e412109f77c45a3bc1c5870b880492fb86a14',
            '\x8b4334d4812c530574bd4f2763fcd22de94a969b')) THEN 'Tokemak'
        WHEN ("to"  IN ('\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f') OR "from" IN ('\xfa82f1ba00b0697227e2ad6c668abb4c50ca0b1f')) THEN 'Jones DAO'
        WHEN ("to"  IN (
          '\x51c2cef9efa48e08557a361b52db34061c025a1b',
          '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277'
        ) OR "from" IN (
            '\x51c2cef9efa48e08557a361b52db34061c025a1b',
            '\x23d2ddae7a638ef8f07a482e31a7f08ba57c1277')) THEN 'JPEGd'
        WHEN ("to"  IN ('\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6') OR "from" IN ('\x6468f283f9d71d2dc28020c0dbe4458f3b47a2c6')) THEN 'Umami Finance'
        ELSE Null
      END AS "DAO",
    "to",
    "from",
    evt_tx_hash
  FROM all_cvx_xfers
  WHERE amount > 1
  ),
cvx_flow AS (
    SELECT *,
        CASE WHEN action = 'Stake' or action = 'Lock' THEN amount ELSE -1*amount END AS "Stake/Lock Delta"
    FROM cvx_xfer_actions
    WHERE action != 'Other'
),
agg_dao_balance AS (
    SELECT
        cvx_flow."date",
        SUM(cvx_flow."Stake/Lock Delta") OVER (PARTITION BY cvx_flow."DAO" ORDER BY cvx_flow."date") AS "DAO Stake/Lock Balance",
        cvx_flow."DAO" AS "DAO",
        CVX_holdings."Wallet CVX" AS "Wallet Holdings"
    FROM cvx_flow
    
    JOIN CVX_holdings ON CVX_holdings."DAO" = cvx_flow."DAO"
    -- GROUP BY 1,3,4,cvx_flow."Stake/Lock Delta"
)

SELECT
        cvx_flow."date",
        SUM(cvx_flow."Stake/Lock Delta") OVER (PARTITION BY cvx_flow."DAO" ORDER BY cvx_flow."date") AS "DAO Stake/Lock Balance",
        cvx_flow."DAO" AS "DAO"
        -- CVX_holdings."Wallet CVX" AS "Wallet Holdings"
FROM cvx_flow
-- SELECT * from CVX_holdings
-- GROUP BY day

-- SELECT
--     "date",
--     "DAO",
--     "Wallet Holdings",
--     "DAO Stake/Lock Balance",
--     "Wallet Holdings" + "DAO Stake/Lock Balance" AS "Total Balance"
-- FROM agg_dao_balance


-- SELECT * from cvx_xfer_actions
-- WHERE action != 'Other'
