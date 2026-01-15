# Blockchain Event Handler Implementation Plan

## Executive Summary

This document outlines the implementation plan for the blockchain event handler in the Rust node. The existing Rust code (currently commented out in `blockchain_event_controller/mod.rs`) was written 2 years ago and needs significant updates to align with the current JS implementation in `dkg-engine`.

## Analysis: Current State

### JavaScript Implementation (Current/Reference)

The JS implementation uses a two-tier command structure:

1. **`EventListenerCommand`** - Periodic scheduler (parent)
   - Runs every 10s (mainnet) or 4s (dev)
   - Spawns `BlockchainEventListenerCommand` for each blockchain

2. **`BlockchainEventListenerCommand`** - Per-blockchain handler
   - Fetches events via `eth_getLogs` in batches (up to 50 blocks)
   - Stores events in `blockchain_event` table
   - Processes events (independent in parallel, dependent sequentially)
   - Uses database transactions for atomicity

**Monitored Contracts & Events (JS - Current):**
| Contract | Events |
|----------|--------|
| Hub | NewContract, ContractChanged, NewAssetStorage, AssetStorageChanged |
| ParametersStorage | ParameterChanged |
| KnowledgeCollectionStorage | KnowledgeCollectionCreated |

**Event Processing Logic:**
- Independent events: Processed in parallel
- Dependent events: Sorted by (blockNumber, transactionIndex, logIndex) and processed sequentially
- Contract change events invalidate old contract addresses and re-fetch events from new contracts

### Rust Implementation (Outdated - 2 years old)

The existing Rust code monitors different contracts:
| Contract | Events |
|----------|--------|
| Hub | NewContract, ContractChanged, NewAssetStorage, AssetStorageChanged |
| ShardingTable | NodeAdded, NodeRemoved |
| Staking | StakeIncreased, StakeWithdrawalStarted |
| Profile | AskUpdated |
| CommitManagerV1U1 | StateFinalized |
| ServiceAgreementV1 | Extended, Terminated |

**Key differences from JS:**
- No `ParametersStorage` or `KnowledgeCollectionStorage` support
- No independent/dependent event classification
- No event persistence in database (processes in-memory)
- No transaction support for atomicity
- No invalidated contracts tracking
- No event re-fetching after contract changes

## Implementation Plan

### Phase 1: Database Schema Updates

#### 1.1 Create `blockchain_event` table migration

```rust
// managers/repository/src/migrations/m007_create_blockchain_event.rs
```

Schema:
```sql
CREATE TABLE blockchain_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blockchain TEXT NOT NULL,
    contract TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    event TEXT NOT NULL,
    data TEXT NOT NULL,  -- JSON serialized event data
    block_number INTEGER NOT NULL,
    transaction_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    tx_hash TEXT NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(blockchain, tx_hash, log_index)
);
CREATE INDEX idx_blockchain_event_processed ON blockchain_event(blockchain, processed);
CREATE INDEX idx_blockchain_event_ordering ON blockchain_event(block_number, transaction_index, log_index);
```

#### 1.2 Update blockchain tracking table

The existing `blockchain` table tracks `last_checked_block` per contract, which is correct. No changes needed.

### Phase 2: Update Contract & Event Definitions

#### 2.1 Update `ContractName` enum

```rust
// managers/blockchain/src/blockchains/abstract_blockchain.rs
pub enum ContractName {
    Hub,
    ParametersStorage,          // NEW
    KnowledgeCollectionStorage, // NEW
    // Remove: ShardingTable, Staking, Profile, etc. (not monitored in current JS)
}
```

#### 2.2 Update `EventName` enum

```rust
pub enum EventName {
    // Hub events
    NewContract,
    ContractChanged,
    NewAssetStorage,
    AssetStorageChanged,
    // ParametersStorage events
    ParameterChanged,           // NEW
    // KnowledgeCollectionStorage events
    KnowledgeCollectionCreated, // NEW
}
```

#### 2.3 Add ABI files

Need to add:
- `abi/ParametersStorage.json`
- `abi/KnowledgeCollectionStorage.json`

### Phase 3: Repository Layer

#### 3.1 Create BlockchainEvent model

```rust
// managers/repository/src/models/blockchain_event.rs
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "blockchain_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i64,
    pub blockchain: String,
    pub contract: String,
    pub contract_address: String,
    pub event: String,
    pub data: String,  // JSON
    pub block_number: i64,
    pub transaction_index: i32,
    pub log_index: i32,
    pub tx_hash: String,
    pub processed: bool,
    pub created_at: DateTimeUtc,
}
```

#### 3.2 Create BlockchainEventRepository

```rust
// managers/repository/src/repositories/blockchain_event_repository.rs
impl BlockchainEventRepository {
    async fn insert_events(&self, events: Vec<Model>, tx: &DatabaseTransaction) -> Result<()>;
    async fn get_unprocessed_events(&self, blockchain: &str, tx: &DatabaseTransaction) -> Result<Vec<Model>>;
    async fn mark_all_as_processed(&self, blockchain: &str) -> Result<()>;
    async fn remove_events_after_block(
        &self,
        blockchain: &str,
        contract: &str,
        contract_address: &str,
        block_number: i64,
        transaction_index: i32,
        tx: &DatabaseTransaction
    ) -> Result<()>;
}
```

### Phase 4: Blockchain Event Service

#### 4.1 Create blockchain events fetching service

```rust
// managers/blockchain/src/events/mod.rs
pub struct BlockchainEventsService {
    contracts: HashMap<String, Address>, // contractName -> address
}

impl BlockchainEventsService {
    pub fn get_contract_address(&self, contract_name: &str) -> Option<Address>;
    pub fn update_contract_address(&mut self, contract_name: &str, address: Address);
    pub async fn get_past_events(
        &self,
        provider: &BlockchainProvider,
        contract_names: &[String],
        events_to_filter: &[String],
        from_block: u64,
        to_block: u64,
    ) -> Result<(Vec<BlockchainEvent>, bool)>; // (events, events_missed)
}
```

### Phase 5: Command Structure (Align with JS)

#### 5.1 Create `EventListenerCommand` (periodic scheduler)

```rust
// src/commands/event_listener_command.rs
pub struct EventListenerCommandHandler {
    context: Arc<Context>,
}

impl CommandHandler for EventListenerCommandHandler {
    fn name(&self) -> &'static str { "eventListenerCommand" }

    fn schedule_config(&self) -> ScheduleConfig {
        // 10s mainnet, 4s dev
        ScheduleConfig::periodic(calculate_period())
    }

    async fn execute(&self, _: &Command) -> CommandExecutionResult {
        // For each blockchain, schedule blockchainEventListenerCommand
        for blockchain in self.context.blockchain_manager().get_blockchain_names() {
            self.schedule_blockchain_listener(blockchain).await;
        }
        CommandExecutionResult::Repeat
    }
}
```

#### 5.2 Create `BlockchainEventListenerCommand` (per-blockchain processor)

```rust
// src/commands/blockchain_event_listener_command.rs
#[derive(Serialize, Deserialize)]
pub struct BlockchainEventListenerCommandData {
    pub blockchain_id: String,
}

pub struct BlockchainEventListenerCommandHandler {
    context: Arc<Context>,
    invalidated_contracts: Arc<RwLock<HashSet<String>>>,
}

impl CommandHandler for BlockchainEventListenerCommandHandler {
    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = BlockchainEventListenerCommandData::from_command(command);

        // Use database transaction
        let tx = self.repository_manager.transaction().await?;

        match self.fetch_and_handle_events(&data.blockchain_id, &tx).await {
            Ok(()) => {
                tx.commit().await?;
                self.mark_all_processed(&data.blockchain_id).await?;
            }
            Err(e) => {
                tx.rollback().await?;
                return CommandExecutionResult::Retry;
            }
        }

        CommandExecutionResult::Completed
    }
}
```

### Phase 6: Event Handlers

#### 6.1 Event classification

```rust
// src/commands/blockchain_event_listener_command.rs
fn is_independent_event(contract: &str, event: &str) -> bool {
    // Currently in JS: CONTRACT_INDEPENDENT_EVENTS = {} (all are dependent)
    false
}
```

#### 6.2 Event handlers

```rust
impl BlockchainEventListenerCommandHandler {
    async fn handle_parameter_changed(&self, event: &BlockchainEvent) {
        // Update contract call cache
    }

    async fn handle_new_contract(&self, event: &BlockchainEvent, current_block: u64, tx: &DatabaseTransaction) {
        // 1. Update blockchain module contract address
        // 2. Update events service contract address
        // 3. Invalidate old contract address
        // 4. Remove events from old contract after this block
        // 5. Re-fetch events from new contract
    }

    async fn handle_contract_changed(&self, event: &BlockchainEvent, current_block: u64, tx: &DatabaseTransaction) {
        // Same as handle_new_contract
    }

    async fn handle_new_asset_storage(&self, event: &BlockchainEvent, current_block: u64, tx: &DatabaseTransaction) {
        // Similar but uses initializeAssetStorageContract
    }

    async fn handle_asset_storage_changed(&self, event: &BlockchainEvent, current_block: u64, tx: &DatabaseTransaction) {
        // Same as handle_new_asset_storage
    }

    async fn handle_knowledge_collection_created(&self, event: &BlockchainEvent) {
        // Queue publishFinalizationCommand
    }
}
```

#### 6.3 Create `PublishFinalizationCommand`

```rust
// src/commands/protocols/publish/publish_finalization_command.rs
#[derive(Serialize, Deserialize)]
pub struct PublishFinalizationCommandData {
    pub event: BlockchainEventData,
}

impl CommandHandler for PublishFinalizationCommandHandler {
    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = PublishFinalizationCommandData::from_command(command);

        // 1. Parse event data (id, publishOperationId, merkleRoot, byteSize)
        // 2. Derive UAL
        // 3. Get transaction and block timestamp
        // 4. Read cached publish data (with retries)
        // 5. Validate merkle root and assertion size
        // 6. Insert knowledge collection into triple store
        // 7. Send finality ack (if local) or network message (if remote)

        CommandExecutionResult::Completed
    }
}
```

### Phase 7: Integration

#### 7.1 Update `CommandResolver`

```rust
// src/commands/command_resolver.rs
let handlers: Vec<Arc<dyn CommandHandler>> = vec![
    // Existing commands...
    Arc::new(EventListenerCommandHandler::new(Arc::clone(&context))),
    Arc::new(BlockchainEventListenerCommandHandler::new(Arc::clone(&context))),
    Arc::new(PublishFinalizationCommandHandler::new(Arc::clone(&context))),
];
```

#### 7.2 Remove old `BlockchainEventController`

Delete or keep commented the old controller in `src/controllers/blockchain_event_controller/mod.rs`.

### Phase 8: Configuration

#### 8.1 Add constants

```rust
// src/commands/constants.rs
pub const CONTRACT_EVENT_FETCH_INTERVAL_MAINNET_MS: i64 = 10_000;
pub const CONTRACT_EVENT_FETCH_INTERVAL_DEV_MS: i64 = 4_000;
pub const MAXIMUM_FETCH_EVENTS_FAILED_COUNT: i32 = 1000;
pub const MAX_BLOCKCHAIN_EVENT_SYNC_BLOCKS: u64 = 3600; // ~1 hour worth
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        CommandExecutor                           │
│  (listens for commands, executes handlers)                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────────────┐
│EventListener  │  │Blockchain     │  │PublishFinalization    │
│Command        │  │EventListener  │  │Command                │
│(periodic)     │  │Command        │  │(one-shot, queued)     │
└───────┬───────┘  └───────┬───────┘  └───────────────────────┘
        │                  │
        │ spawns per       │
        │ blockchain       │
        └──────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Fetch events via RPC   │
              │ (eth_getLogs, batched) │
              └───────────┬────────────┘
                          │
                          ▼
              ┌────────────────────────┐
              │ Store in blockchain_   │
              │ event table            │
              └───────────┬────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    Independent     Dependent        Dependent
    Events          Events           Events
    (parallel)      (sorted)         ...
                          │
                          ▼
              ┌────────────────────────┐
              │ Event Handlers         │
              │ - ParameterChanged     │
              │ - NewContract          │
              │ - ContractChanged      │
              │ - KnowledgeCollection  │
              │   Created → queues     │
              │   PublishFinalization  │
              └────────────────────────┘
```

## Key Differences from Old Rust Implementation

| Aspect | Old Rust | New Rust (aligned with JS) |
|--------|----------|---------------------------|
| Contracts monitored | Hub, ShardingTable, Staking, Profile, etc. | Hub, ParametersStorage, KnowledgeCollectionStorage |
| Event persistence | None (in-memory) | Database table with processed flag |
| Transaction support | None | Full database transactions |
| Event classification | None | Independent vs Dependent |
| Event ordering | By block only | By block, txIndex, logIndex |
| Contract invalidation | None | Track invalidated contracts, re-fetch |
| Error handling | Unwrap/panic | Proper error propagation with rollback |
| Command structure | Single controller | Two-tier command (scheduler + handler) |

## Testing Strategy

1. **Unit tests**: Event parsing, classification, ordering
2. **Integration tests**: Full event flow with mock blockchain
3. **Manual testing**: Run against Hardhat/testnet

## Migration Notes

- The old `BlockchainEventController` should be removed
- The `blockchain` table schema remains compatible
- New `blockchain_event` table needs migration
- ABI files for new contracts need to be added

## Resolved Questions

### 1. Sharding Table Updates

**Finding**: The JS implementation does **NOT** use blockchain events for sharding table updates. Instead, it uses a **polling-based reconciliation** approach via `ShardingTableCheckCommand`:

- Runs every **10 seconds** (`SHARDING_TABLE_CHECK_COMMAND_FREQUENCY_MILLS`)
- Compares `getShardingTableLength()` from blockchain vs local `getPeersCount()`
- If mismatch detected:
  1. Deletes all local peer records for that blockchain
  2. Re-pulls entire sharding table via paginated `getShardingTablePage()` calls
  3. For each node: converts `nodeId` hex→ascii, computes sha256, inserts record

**Decision**: Implement `ShardingTableCheckCommand` as a **separate command** (not part of blockchain event handler). This is simpler than event-based updates but ensures consistency.

### 2. Event Classification

**Decision**: All events will be treated as **dependent** and processed sequentially in proper order (blockNumber → transactionIndex → logIndex). This is the safest approach and matches the current JS behavior where `CONTRACT_INDEPENDENT_EVENTS = {}`.

### 3. AssetUpdated Events

**Decision**: Will **NOT** be implemented for now as the update functionality is not currently working.

---

## Additional Required Component: ShardingTableCheckCommand

Since sharding table updates are handled separately from blockchain events, we need to implement this as Phase 9.

### Phase 9: Sharding Table Check Command

#### 9.1 Add ShardingTableStorage contract

```rust
// managers/blockchain/src/blockchains/blockchain_creator.rs
abigen!(ShardingTableStorage, "../../abi/ShardingTableStorage.json");
```

#### 9.2 Add to Contracts struct

```rust
struct Contracts {
    // ... existing fields ...
    sharding_table_storage: ShardingTableStorage<BlockchainProvider>,
}
```

#### 9.3 Add BlockchainManager methods

```rust
// managers/blockchain/src/lib.rs
impl BlockchainManager {
    pub async fn get_sharding_table_length(&self, blockchain: &BlockchainName) -> Result<u64, BlockchainError>;
    pub async fn get_sharding_table_head(&self, blockchain: &BlockchainName) -> Result<u128, BlockchainError>;
    pub async fn get_sharding_table_page(
        &self,
        blockchain: &BlockchainName,
        starting_identity_id: u128,
        page_size: u64,
    ) -> Result<Vec<ShardingTableNode>, BlockchainError>;
}

#[derive(Debug, Clone)]
pub struct ShardingTableNode {
    pub identity_id: u128,
    pub node_id: String,  // hex encoded peer ID
    pub ask: U256,
    pub stake: U256,
}
```

#### 9.4 Create ShardingTableCheckCommand

```rust
// src/commands/sharding_table_check_command.rs
const SHARDING_TABLE_CHECK_FREQUENCY_MS: i64 = 10_000; // 10 seconds

#[derive(Serialize, Deserialize)]
pub struct ShardingTableCheckCommandData;

pub struct ShardingTableCheckCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    validation_manager: Arc<ValidationManager>,  // for sha256 hashing
}

impl CommandHandler for ShardingTableCheckCommandHandler {
    fn name(&self) -> &'static str { "shardingTableCheckCommand" }

    fn schedule_config(&self) -> ScheduleConfig {
        ScheduleConfig::periodic(SHARDING_TABLE_CHECK_FREQUENCY_MS)
    }

    async fn execute(&self, _: &Command) -> CommandExecutionResult {
        let tx = self.repository_manager.transaction().await?;

        for blockchain in self.blockchain_manager.get_blockchain_names() {
            let blockchain_count = self.blockchain_manager
                .get_sharding_table_length(blockchain).await?;
            let local_count = self.repository_manager
                .shard_repository()
                .get_peers_count(blockchain.as_str()).await?;

            if blockchain_count as usize != local_count {
                tracing::info!(
                    "Sharding table mismatch for {}: blockchain={}, local={}. Re-syncing...",
                    blockchain, blockchain_count, local_count
                );
                self.pull_blockchain_sharding_table(blockchain, &tx).await?;
            }
        }

        tx.commit().await?;
        CommandExecutionResult::Repeat
    }
}

impl ShardingTableCheckCommandHandler {
    async fn pull_blockchain_sharding_table(
        &self,
        blockchain: &BlockchainName,
        tx: &DatabaseTransaction,
    ) -> Result<(), Error> {
        // 1. Delete all local peer records for this blockchain
        self.repository_manager
            .shard_repository()
            .remove_all_peers(blockchain.as_str(), tx)
            .await?;

        // 2. Get sharding table length and head
        let table_length = self.blockchain_manager
            .get_sharding_table_length(blockchain).await?;
        let mut starting_id = self.blockchain_manager
            .get_sharding_table_head(blockchain).await?;

        const PAGE_SIZE: u64 = 10;
        let mut fetched = 0u64;
        let mut slice_index = 0;

        // 3. Paginate through the sharding table
        while fetched < table_length {
            let nodes = self.blockchain_manager
                .get_sharding_table_page(blockchain, starting_id, PAGE_SIZE)
                .await?;

            // Skip first node on subsequent pages (it's the last from previous)
            for node in nodes.iter().skip(slice_index) {
                if node.node_id == "0x" {
                    continue;
                }

                // Convert hex nodeId to ascii peer ID
                let peer_id = hex_to_ascii(&node.node_id)?;
                let sha256 = self.validation_manager
                    .call_hash_function(&HashFunction::Sha256, peer_id.as_bytes().to_vec());

                self.repository_manager
                    .shard_repository()
                    .create_peer_record(PeerRecord {
                        peer_id,
                        blockchain_id: blockchain.as_str().to_owned(),
                        ask: from_wei(&node.ask.to_string()),
                        stake: from_wei(&node.stake.to_string()),
                        sha256: to_hex_string(sha256),
                        last_seen: DateTime::from_timestamp(0, 0).unwrap(),
                        last_dialed: DateTime::from_timestamp(0, 0).unwrap(),
                    }, tx)
                    .await?;

                fetched += 1;
            }

            if let Some(last) = nodes.last() {
                starting_id = last.identity_id;
            }
            slice_index = 1; // Skip first on subsequent pages
        }

        tracing::info!(
            "Pulled {} nodes from blockchain {} sharding table",
            fetched, blockchain
        );
        Ok(())
    }
}
```

#### 9.5 Add to CommandResolver

```rust
// src/commands/command_resolver.rs
let handlers: Vec<Arc<dyn CommandHandler>> = vec![
    // ... existing handlers ...
    Arc::new(ShardingTableCheckCommandHandler::new(Arc::clone(&context))),
];
```

#### 9.6 Required Repository additions

```rust
// managers/repository/src/repositories/shard_repository.rs
impl ShardRepository {
    pub async fn get_peers_count(&self, blockchain_id: &str) -> Result<usize>;
    pub async fn remove_all_peers(&self, blockchain_id: &str, tx: &DatabaseTransaction) -> Result<()>;
    pub async fn create_peer_record(&self, record: Model, tx: &DatabaseTransaction) -> Result<()>;
}
```

---

## Updated Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           CommandExecutor                                │
│  (listens for commands, executes handlers)                               │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
    ┌───────────────┬───────────┼───────────────┬───────────────┐
    ▼               ▼           ▼               ▼               ▼
┌─────────┐  ┌───────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐
│Event    │  │Sharding   │  │Blockchain   │  │Publish      │  │Dial     │
│Listener │  │TableCheck │  │EventListener│  │Finalization │  │Peers    │
│Command  │  │Command    │  │Command      │  │Command      │  │Command  │
│(10s/4s) │  │(10s)      │  │(one-shot)   │  │(one-shot)   │  │(30s)    │
└────┬────┘  └─────┬─────┘  └──────┬──────┘  └─────────────┘  └─────────┘
     │             │               │
     │ spawns      │ polls         │ processes
     │ per BC      │ BC vs local   │ events
     │             │               │
     ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Blockchain RPC                                   │
│  - eth_getLogs (events)                                                  │
│  - ShardingTableStorage.nodesCount(), head()                            │
│  - ShardingTable.getShardingTable(startId, pageSize)                    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Summary of All Components

| Component | Type | Period | Purpose |
|-----------|------|--------|---------|
| EventListenerCommand | Periodic | 10s/4s | Spawns BlockchainEventListenerCommand per blockchain |
| BlockchainEventListenerCommand | One-shot | N/A | Fetches & processes blockchain events |
| ShardingTableCheckCommand | Periodic | 10s | Reconciles local sharding table with blockchain |
| PublishFinalizationCommand | One-shot | N/A | Handles KnowledgeCollectionCreated events |
| DialPeersCommand | Periodic | 30s | (existing) Dials peers from sharding table |
