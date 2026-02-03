-- Initialize tracked addresses for Goldsky pipeline
-- Run this against your PostgreSQL database after deploying the pipeline

-- The table is created automatically by Goldsky in the 'streamling' schema
-- Table structure: streamling.tracked_addresses (contract_address VARCHAR)

-- Insert all 54 addresses (lowercase for consistent matching)
INSERT INTO streamling.tracked_addresses (contract_address) VALUES
('0xfd13d0c145f4b553c1d2c0ad14dfa250b926b52e'),
('0x999925b100da18020b93aec850723c25d674cf16'),
('0xd92ce95d7144e19ad1cb6765b26bd25203f29bbf'),
('0x1f64c3c24891b6caec88edd2ebf89514167de8da'),
('0x4b1ab1e528354dc9730902256f6af83d2d6d935b'),
('0xda1a91758d9fa7a15ae98efb054bc6bf8c6b5c65'),
('0x10b345de378e69668a41137f8bd5b657a191d174'),
('0x4e61e15524316f8e293daa85dc50dbabe2ee7d2e'),
('0xf5c89bfee1ca1b89594b268f7bdbb775e39b8ced'),
('0x061f65731068f9439ce2400b187bc49d87ae1044'),
('0xd64beafa2b4a1a6427aa11aa836d516a5cbe7fa5'),
('0x034b5dbd5adb0b1287371cb274d8e3d134aaa45f'),
('0x4e47fb10924c884ba6fbeda4bce778f4d5d2ca71'),
('0x37b9444af5e4b14bc0e1ebe815fd1df9998ded2e'),
('0xe6bb2e05dfde09260aa5fbb1df52a023ee73e990'),
('0xf4a6d658abcee18204aa62bcdf5f67992bbdf45e'),
('0x331f2bb09dad495f9a9420bb1bbd88c58967c46d'),
('0x34c46f3cdb7cb09878d8c02af5d92bf84c1de303'),
('0xab604b0b5066e58bdb286ec0fc2f1183430b7d60'),
('0x94a80b173b234882f30ce7757d9c1bb88d194c99'),
('0x124963385f48fe326831953af187c19770fd30da'),
('0x8e553968583ab2731d894ec9ade42e1731fffc41'),
('0x7a1475076a5ec895a80ceb50c33c965a8ea38e06'),
('0xc8bd6b537aa22551e16d8bdc40053b47de039b2c'),
('0x74a8589752d309daa83685f68daaaf50aba10814'),
('0xc71553cff0a45239fe4bb9a7115615628c30b103'),
('0xcfc69ecaa8046bcb2879eb6565efdf69b3afec03'),
('0xb6dd4cd17cf4ab850944fe368a40c1490699cf81'),
('0xe4a6a1afeebd468543cc76e67357cadbe73ab696'),
('0x46dfad29ca251c62a06ed426295d3c196e3fac91'),
('0xa2afecff0d7d0356aaa61cc73c976422cefefa58'),
('0x8ca1b9822d4cb1e2b5d8fa61018b070fba0c22ca'),
('0xd2bd8d649662d8da78456433127b860a99776b47'),
('0x0ae8210eb304c28f83ddc48954bd3a720d023ba7'),
('0xe2e12516968632ef1c4c0322e370032bc8a17780'),
('0x7808b8332d3f979a52cfbd15bec62eeebb8fd0c0'),
('0x87b902b647cc2e0b068e9c66ccc62e3da6ccbe1a'),
('0x64e919bb292e0e22b712258b9f89086596cc48fd'),
('0xd7eff104c2b137a98419e91df0fac9d0fd3a912b'),
('0xdde6a1e386127909c3e773d251402a857fd2e87b'),
('0xe6955f929604fc02bed05332683b0d25d5cf1f2f'),
('0xb544c98acbd659b7394792b41577d1a30aaf99be'),
('0x7349c061335e15501f75beb7279d4c16fb122f44'),
('0xa7b7cc7b015a5ae5eb1098c0cfde4fadd6018231'),
('0xc252dadef13dd8f8597803090c14bf6f4c0c35e5'),
('0x9aa458df519fb0085f44913c7bb5ccdea7fe2b47'),
('0x0bad88f7d2841efe3a2ccc5105a5476bbb2399f6'),
('0xf67cce6a480f7f41fe864c8e38b246d346426182'),
('0x16ca842c2493a75cb4046cd8c1c45b962d981d1d'),
('0x13ae5b0abf95c2ae604568f1c608712b9da48f43'),
('0x0f981da41a034787e50805373e3185c90373c8f5'),
('0x490dd891af6132ff6fc98b1298a5802983f08813'),
('0xdc694fec371beaba6af58bb3f28b135a4a0fa4b3'),
('0x8efc306a77bb56b31ff5a1697f288e1c9588b833')
ON CONFLICT DO NOTHING;

-- Verify count
SELECT COUNT(*) as total_addresses FROM streamling.tracked_addresses;
