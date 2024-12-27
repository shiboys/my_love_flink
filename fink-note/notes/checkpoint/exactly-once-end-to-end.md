# 端到端的精确一次

## 背景

通过 checkpoint 的流程分析，我们发现 checkpoint barrier 的对齐实现了 flink 内部上下游算子之间的 exactly-once(统计意义上的)，那么如何针对端到端的 exactly-once (比如在 flink 的 process function 中，我们将消息通过短信发出去或者通过 sink 到其他的 kafka topic 中 )，flink 如何实现 exactly-once 那？

## Flink 对 Exactly Once 的支持

Flink 1.4 版本之前
 * 支持 Exactly Once 语义，仅限于应用程序内部

Flink 1.4 之后
  * 通过两阶段提交(TwoPhaseCommitSinkFunction) 来支持 end-to-end Exactly Once
  * kafka 0.11+(才支持两阶段提交的分布式事务)
  
分布式快照
  * 输入流的位置(source)
  * 当期状态(source以及下游的各个 operator 的状态)


## 两阶段提交

在分布式的系统中，我们采用两阶段提交协议来实现数据的一致性，两阶段提交就是把提交操作分为两个步骤，一个是预提交阶段，一个是提交阶段，通常这里包含两个角色，一个是master为协调管理者，一个是slave为执行者，提交过程如下：

1、master发出预提交命令给所有的slave

2、slave执行预提交的命令，执行完后给master发送一个ack反馈信息

3、当master收到所有的slave的成功反馈信息，那么再次给slave发送提交信息commit

4、slave执行commit操作

如果在上面的流程中，在流程2上出现问题，也就是预提交出现问题，那么master会收到slave的失败反馈，这个时候master会让slave进行回滚操作，保证数据的一致性，但是在流程4中出现问题，那么就会造成数据的不一致性，这个时候我们可以采用3次提交技术或者其他的方式进行数据修正，来达到最终的一致性。



## Exactly Once 的核心类

我们先来看看 Flink 是如果根据现有的 checkpoint 机制去实现 2PC 的，首先来看两个常见的核心接口：CheckpointedFunction 、CheckpointListener

**CheckpointedFunction**

```java
public interface CheckpointedFunction {
	void snapshotState(FunctionSnapshotContext context) throws Exception;

	void initializeState(FunctionInitializationContext context) throws Exception;
}
```

snapshotState 方法，每次 checkpoint 触发执行方法，通常会将缓存数据(本地变量)放入状态中，可以理解为是一个 hook，这个方法里面可以实现预提交（Checkpoint 的 snapshotState 中可以实现预提交，看到这里是不是有点懵逼？懵逼就对了，接下来我们看看 Flink 的 TowPhaseCommitSinkFunction 如何利用这一特点来实现 2PC 的化腐朽为神奇的代码）

对比flink 整个checkpoint机制调用流程可以发现与2PC非常相似，JobMaster相当于master协调者，所有的处理节点相当于slave执行者，start-checkpoint消息相当于pre-commit消息，每个处理节点的checkpoint相当于pre-commit过程，checkpoint ack消息相当于执行者反馈信息，最后callback消息相当于commit消息，完成具体的提交动作。

**CheckpointListener**

```java
public interface CheckpointListener {

    // Checkpoint 完成之后的通知方法，这里可用做一些额外的操作
	void notifyCheckpointComplete(long checkpointId) throws Exception;

	default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}
```

notifyCheckpointComplete 方法，Checkpoint 完成之后的通知方法，这里可用做一些额外的操作，例如 FlinkKafkaConsumerBase 使用这个来完成 kafka offset 的提交，在这个方法里面可以实现提交操作。

在两阶段提价中如果流程 2 与提交失败，那么本次 checkpoint 就会被取消执行，不会影响数据的一致性，如果流程 4 提交失败了，在 flink 中可以怎么处理那？我们可以在预提交阶段(snapshotState)将事务信息保存在 state 状态中，如果流程 4 失败，那么就可以从状态中恢复事务信息，并且在 CheckpointedFunction 的 initializeState 方法中完成事务的提交，该方法是初始化方法(重启或者首次初始化)，只会执行一次，从而保证数据的一致性

## TwoPhaseCommitSinkFunction

这是一个抽象类，继承了 RichSinkFunction，并且实现了我们上面提到的两个接口，在这个抽象类中，一共有 5 个抽象方法需要子类去实现

```java
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener {
        
    /**
	 * Method that starts a new transaction.
	 *
	 * @return newly created transaction.
     * 开启一个事务，获取一个句柄
	 */
    protected abstract TXN beginTransaction() throws Exception;
        
    /**
	 * Write value within a transaction.
	 */
	protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;


	/**
	 * Pre commit previously created transaction. Pre commit must make all of the necessary steps to prepare the
	 * transaction for a commit that might happen in the future. After this point the transaction might still be
	 * aborted, but underlying implementation must ensure that commit calls on already pre committed transactions
	 * will always succeed.
	 *
	 * <p>Usually implementation involves flushing the data.
     * 预提交———提交预执行
	 */
	protected abstract void preCommit(TXN transaction) throws Exception;

	/**
	 * Commit a pre-committed transaction. If this method fail, Flink application will be
	 * restarted and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
	 * same transaction.
     * 提交执行
	 */
	protected abstract void commit(TXN transaction);

	/**
	 * Abort a transaction.
     * 放弃一个事务
	 */
	protected abstract void abort(TXN transaction);
}
```
从上面的代码中可以看到，核心的四个抽象方法组成了提交的整个过程
1、beginTransaction 这是开始一个事务

2、preCommit 预提交操作

3、commit 提价操作

4、abort 终止放弃一个事务

通过上面的 4 个方法和 checkpoint 相关的方法，来实现两阶段提交的实现过程，下面我们来分析下 initializeState 的源码

### initializeState 方法

```java
public void initializeState(FunctionInitializationContext context) throws Exception {
		// when we are restoring state with pendingCommitTransactions, we don't really know whether the
		// transactions were already committed, or whether there was a failure between
		// completing the checkpoint on the master, and notifying the writer here.

		// (the common case is actually that is was already committed, the window
		// between the commit on the master and the notification here is very small)

		// it is possible to not have any transactions at all if there was a failure before
		// the first completed checkpoint, or in case of a scale-out event, where some of the
		// new task do not have and transactions assigned to check)

		// we can have more than one transaction to check in case of a scale-in event, or
		// for the reasons discussed in the 'notifyCheckpointComplete()' method.
        // 获取 "state" 的 state
		state = context.getOperatorStateStore().getListState(stateDescriptor);

		boolean recoveredUserContext = false;
		if (context.isRestored()) { // 开始从 checkpoint 恢复
			LOG.info("{} - restoring state", name());
			for (State<TXN, CONTEXT> operatorState : state.get()) {
				userContext = operatorState.getContext();
				List<TransactionHolder<TXN>> recoveredTransactions = operatorState.getPendingCommitTransactions();
				List<TXN> handledTransactions = new ArrayList<>(recoveredTransactions.size() + 1);
				for (TransactionHolder<TXN> recoveredTransaction : recoveredTransactions) {
					// If this fails to succeed eventually, there is actually data loss
                    // 恢复 之前的挂起的已经执行完预提交的事务列表
					recoverAndCommitInternal(recoveredTransaction);
					handledTransactions.add(recoveredTransaction.handle);
					LOG.info("{} committed recovered transaction {}", name(), recoveredTransaction);
				}

				{
                    // 放弃刚开始还没有预提交的事务。在 checkpoint 的时候 ,该 sink function 的做法是
                    // 先预提交当前事务，然后再开一个新事务
					TXN transaction = operatorState.getPendingTransaction().handle;
					recoverAndAbort(transaction);
					handledTransactions.add(transaction);
					LOG.info("{} aborted recovered transaction {}", name(), operatorState.getPendingTransaction());
				}

				if (userContext.isPresent()) {
                    // 恢复用户上下文
					finishRecoveringContext(handledTransactions);
					recoveredUserContext = true;
				}
			}
		}
        // 不论是 checkpoint 恢复，还是初始化，下面的代码逻辑都要执行
		// if in restore we didn't get any userContext or we are initializing from scratch
		if (!recoveredUserContext) {
			LOG.info("{} - no state to restore", name());

			userContext = initializeUserContext();
		}
		this.pendingCommitTransactions.clear();
        // 初始化一个新的事务
		currentTransactionHolder = beginTransactionInternal();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);
	}
```

从上面的方法中我们可以看到这个方法中主要有两件事，第一件事就是关于状态的恢复方面的操作和判断，第二件事就是开启一个事务，然后获取其句柄。关于恢复方面，如果上次 事务 完成预提交，那么说明该 checkpoint 已经完成，可以执行 commit 操作；如果是下一次 checkpoint 开始的事务，那说明 checkpoint 过程中出现了问题，执行 abort 操作。

### snapshotState 方法

```java
    @Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// this is like the pre-commit of a 2-phase-commit transaction
		// we are ready to commit and remember the transaction

		checkState(currentTransactionHolder != null, "bug: no transaction object when performing state snapshot");
        // 获取 checkpoint Id
		long checkpointId = context.getCheckpointId();
		LOG.debug("{} - checkpoint {} triggered, flushing transaction '{}'", name(), context.getCheckpointId(), currentTransactionHolder);
        // 执行预提交操作
		preCommit(currentTransactionHolder.handle);
		pendingCommitTransactions.put(checkpointId, currentTransactionHolder);
		LOG.debug("{} - stored pending transactions {}", name(), pendingCommitTransactions);
        // 开启一个新的事务，供下次 checkpoint 使用
		currentTransactionHolder = beginTransactionInternal();
		LOG.debug("{} - started new transaction '{}'", name(), currentTransactionHolder);

		state.clear();
        // 把这两个句柄放到 state 中进行容错。
		state.add(new State<>(
			this.currentTransactionHolder,
			new ArrayList<>(pendingCommitTransactions.values()),
			userContext));
	}

    public static final class State<TXN, CONTEXT> {
		protected TransactionHolder<TXN> pendingTransaction;
		protected List<TransactionHolder<TXN>> pendingCommitTransactions = new ArrayList<>();
		protected Optional<CONTEXT> context;

		public State() {
		}

		public State(TransactionHolder<TXN> pendingTransaction, List<TransactionHolder<TXN>> pendingCommitTransactions, Optional<CONTEXT> context) {
			this.context = requireNonNull(context, "context is null");
			this.pendingTransaction = requireNonNull(pendingTransaction, "pendingTransaction is null");
			this.pendingCommitTransactions = requireNonNull(pendingCommitTransactions, "pendingCommitTransactions is null");
		}
    }
```

### notifyCheckpointComplete 方法

```java
@Override
	public final void notifyCheckpointComplete(long checkpointId) throws Exception {
		// the following scenarios are possible here
		//
		//  (1) there is exactly one transaction from the latest checkpoint that
		//      was triggered and completed. That should be the common case.
		//      Simply commit that transaction in that case.
		//
		//  (2) there are multiple pending transactions because one previous
		//      checkpoint was skipped. That is a rare case, but can happen
		//      for example when:
		//
		//        - the master cannot persist the metadata of the last
		//          checkpoint (temporary outage in the storage system) but
		//          could persist a successive checkpoint (the one notified here)
		//
		//        - other tasks could not persist their status during
		//          the previous checkpoint, but did not trigger a failure because they
		//          could hold onto their state and could successfully persist it in
		//          a successive checkpoint (the one notified here)
		//
		//      In both cases, the prior checkpoint never reach a committed state, but
		//      this checkpoint is always expected to subsume the prior one and cover all
		//      changes since the last successful one. As a consequence, we need to commit
		//      all pending transactions.
		//
		//  (3) Multiple transactions are pending, but the checkpoint complete notification
		//      relates not to the latest. That is possible, because notification messages
		//      can be delayed (in an extreme case till arrive after a succeeding checkpoint
		//      was triggered) and because there can be concurrent overlapping checkpoints
		//      (a new one is started before the previous fully finished).
		//
		// ==> There should never be a case where we have no pending transaction here
		//

		Iterator<Map.Entry<Long, TransactionHolder<TXN>>> pendingTransactionIterator = pendingCommitTransactions.entrySet().iterator();
		Throwable firstError = null;

		while (pendingTransactionIterator.hasNext()) {
			Map.Entry<Long, TransactionHolder<TXN>> entry = pendingTransactionIterator.next();
			Long pendingTransactionCheckpointId = entry.getKey();
			TransactionHolder<TXN> pendingTransaction = entry.getValue();
            // 如果当前通知的 checkpointId < 挂起事务的 checkpointId , 说明当前通知的 checkpointId 是一个更早的 checkpoint
            //它的通知回调是不能处理处理晚于它的事务
			if (pendingTransactionCheckpointId > checkpointId) {
				continue;
			}
            //相反，如果挂起事务的 pendingTransactionCheckpointId <= 当前 checkpointId 
            // 说明 当前 checkpoint 可以可以处理它引起的挂起事务或者它之前的挂起事务

			LOG.info("{} - checkpoint {} complete, committing transaction {} from checkpoint {}",
				name(), checkpointId, pendingTransaction, pendingTransactionCheckpointId);

			logWarningIfTimeoutAlmostReached(pendingTransaction);
			try {
                // 执行提交操作
				commit(pendingTransaction.handle);
			} catch (Throwable t) {
				if (firstError == null) {
					firstError = t;
				}
			}

			LOG.debug("{} - committed checkpoint transaction {}", name(), pendingTransaction);

			pendingTransactionIterator.remove();
		}

		if (firstError != null) {
			throw new FlinkRuntimeException("Committing one of transactions failed, logging first encountered failure",
				firstError);
		}
	}
```

从上面的代码中，我们可以看到 notifyCheckpiontComplete 方法中，最核心的就是调用了 commit(pendingTransaction.handle) 方法

在上面的流程中，任何一个步骤都会出现问题

* 预提交出现问题，任务会失败重启回到最近一次的 checkpoint 成功状态，预提交的事务自然会因为事务超时而放弃
* 预提交之后提交之前出现问题，也就是完成 checkpoint 但是还没触发 notifyCheckpointComplete 动作，这个过程失败，那么就会从这次checkpoint 中恢复，执行 initializeState 中的逻辑保证数据的一致性；
* commit 之后下次 checkpoint 之前失败，也就是在执行 notifyCheckpointComplete 之后失败，那么任务重启会继续之前已经提交过的事务，因此事务的提交需要保证提交不会影响数据的一致性，也就是需要外部系统支持幂等性操作。

### 总结

1、外部存储系统需要支持事务操作的特性
2、外部存储系统提供的事务句柄是可以序列化和持久化的，可以重复提交的
3、两阶段提交和 checkpoint 组合在一起的，所以 Checkpoint 的执行周期会导致外部系统数据的延迟周期，比如 10 分钟进行一次 checkpoint，那么外部系统可能会存在 10 分钟左右的延迟。

## TwoPhaseCommitSinkFunction 的实现类 FlinkKafakProducer011 源码分析

下面我们来分析下 TwoPhaseCommitSinkFunction 的实现类 FlinkKafakProducer011 的核心源码，

主要是上面的 5 个方法

### 类名和构造函数

```java
public class FlinkKafkaProducer011<IN>
		extends TwoPhaseCommitSinkFunction<IN, FlinkKafkaProducer011.KafkaTransactionState, FlinkKafkaProducer011.KafkaTransactionContext> {
    public FlinkKafkaProducer011(
        String defaultTopicId,
        KeyedSerializationSchema<IN> serializationSchema,
        Properties producerConfig,
        Optional<FlinkKafkaPartitioner<IN>> customPartitioner) {
    this(
        defaultTopicId,
        serializationSchema,
        producerConfig,
        customPartitioner,
        Semantic.AT_LEAST_ONCE, // 可以看出默认使用的 AT_LEAST_ONCE 语义，如果要实现我们本章节的 Exactly-Once 语义，需要我们手工传入 Sematic.EXACTLY_ONCE 参数
        DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
    }

    // 这个类 是 TwoPhaseCommitSinkFunction 中的 TXN 具体实现类，可以看到
    // FlinkKafkaProducer 的这个 TXN 实现类将 transactionId、producerId、epoch 和 producer 进行了封装
    // 注意看， producer 被 transient 修饰，说明这个对象并不会被序列化到 checkpoint 的 state 中，
    // 既然不会被序列化，那么从 checkpoint 恢复的话，就不会恢复该对象，那该对象是核心的 producer 类是如何被还原回来的那？
    @VisibleForTesting
	@Internal
	public static class KafkaTransactionState {

		private final transient FlinkKafkaProducer<byte[], byte[]> producer;

		@Nullable
		final String transactionalId;

		final long producerId;

		final short epoch;

		@VisibleForTesting
		@Internal
		public KafkaTransactionState(String transactionalId, FlinkKafkaProducer<byte[], byte[]> producer) {
			this(transactionalId, producer.getProducerId(), producer.getEpoch(), producer);
		}

		@VisibleForTesting
		@Internal
		public KafkaTransactionState(FlinkKafkaProducer<byte[], byte[]> producer) {
			this(null, -1, (short) -1, producer);
		}

		@VisibleForTesting
		@Internal
		public KafkaTransactionState(
				@Nullable String transactionalId,
				long producerId,
				short epoch,
				FlinkKafkaProducer<byte[], byte[]> producer) {
			this.transactionalId = transactionalId;
			this.producerId = producerId;
			this.epoch = epoch;
			this.producer = producer;
		}
    }
    
    public FlinkKafkaProducer011(
			String defaultTopicId,
			KeyedSerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			Optional<FlinkKafkaPartitioner<IN>> customPartitioner,
			Semantic semantic,
			int kafkaProducersPoolSize) {
		super(new TransactionStateSerializer(), new ContextStateSerializer());

		this.defaultTopicId = checkNotNull(defaultTopicId, "defaultTopicId is null");
		this.schema = checkNotNull(serializationSchema, "serializationSchema is null");
		this.producerConfig = checkNotNull(producerConfig, "producerConfig is null");
		this.flinkKafkaPartitioner = checkNotNull(customPartitioner, "customPartitioner is null").orElse(null);
		this.semantic = checkNotNull(semantic, "semantic is null");
		this.kafkaProducersPoolSize = kafkaProducersPoolSize;
		checkState(kafkaProducersPoolSize > 0, "kafkaProducersPoolSize must be non empty");

		ClosureCleaner.clean(this.flinkKafkaPartitioner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
		ClosureCleaner.ensureSerializable(serializationSchema);

		// set the producer configuration properties for kafka record key value serializers.
		if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		// eagerly ensure that bootstrap servers are set.
		if (!this.producerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be supplied in the producer config properties.");
		}

		if (!producerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
			long timeout = DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMilliseconds();
			checkState(timeout < Integer.MAX_VALUE && timeout > 0, "timeout does not fit into 32 bit integer");
			this.producerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) timeout);
			LOG.warn("Property [{}] not specified. Setting it to {}", ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
		}

		// Enable transactionTimeoutWarnings to avoid silent data loss
		// See KAFKA-6119 (affects versions 0.11.0.0 and 0.11.0.1):
		// The KafkaProducer may not throw an exception if the transaction failed to commit
        // 设置 Kafka 的 transaction.timeout.ms 参数，注意默认这里使用了 flink kafka 的DEFAULT_KAFKA_TRANSACTION_TIMEOUT
        // 参数值，该参数值为 1 小时
		if (semantic == Semantic.EXACTLY_ONCE) {
			final Object object = this.producerConfig.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
			final long transactionTimeout;
			if (object instanceof String && StringUtils.isNumeric((String) object)) {
				transactionTimeout = Long.parseLong((String) object);
			} else if (object instanceof Number) {
				transactionTimeout = ((Number) object).longValue();
			} else {
				throw new IllegalArgumentException(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
					+ " must be numeric, was " + object);
			}
			super.setTransactionTimeout(transactionTimeout);
			super.enableTransactionTimeoutWarnings(0.8);
		}

		this.topicPartitionsMap = new HashMap<>();
	}

    @Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		if (semantic != Semantic.NONE && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Using {} semantic, but checkpointing is not enabled. Switching to {} semantic.", semantic, Semantic.NONE);
			semantic = Semantic.NONE;
		}

		migrateNextTransactionalIdHindState(context);

		transactionalIdsGenerator = new TransactionalIdsGenerator(
			getRuntimeContext().getTaskName() + "-" + ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID(),
			getRuntimeContext().getIndexOfThisSubtask(),
			getRuntimeContext().getNumberOfParallelSubtasks(),
			kafkaProducersPoolSize,
			SAFE_SCALE_DOWN_FACTOR);

		if (semantic != Semantic.EXACTLY_ONCE) {
			nextTransactionalIdHint = null;
		} else {
			ArrayList<NextTransactionalIdHint> transactionalIdHints = Lists.newArrayList(nextTransactionalIdHintState.get());
            // transactionalIdHints 封装了下一个事务要使用的 事务Id 
			if (transactionalIdHints.size() > 1) { // 最多只能有 1 个
				throw new IllegalStateException(
					"There should be at most one next transactional id hint written by the first subtask");
			} else if (transactionalIdHints.size() == 0) { // 刚开始初始化
				nextTransactionalIdHint = new NextTransactionalIdHint(0, 0); // 从 0 开始计算

				// this means that this is either:
				// (1) the first execution of this application
				// (2) previous execution has failed before first checkpoint completed
				//
				// in case of (2) we have to abort all previous transactions
				abortTransactions(transactionalIdsGenerator.generateIdsToAbort());
			} else {
				nextTransactionalIdHint = transactionalIdHints.get(0);
			}
		}
        // 调用 TowPhaseCommitSinkFunction 的 initializeState 方法，提交挂起的事务，并启动新事务，这里会调用 beginTransaction，
        // 因此启动的是 Kafka 的事务，下面我们看看 FlinkKafkaProducer 是如何启动 kafka 的事务的
		super.initializeState(context);
	}

    // 恢复用户的上下文
    @Override
	protected Optional<KafkaTransactionContext> initializeUserContext() {
		if (semantic != Semantic.EXACTLY_ONCE) {
			return Optional.empty();
		}

		Set<String> transactionalIds = generateNewTransactionalIds();
		resetAvailableTransactionalIdsPool(transactionalIds);
		return Optional.of(new KafkaTransactionContext(transactionalIds));
	}

    // 以 nextTransactionalIdHint 中的 beginTransactionId 作为 baseId,
    // 产生 poolSize 个  transactionId 以提供 kafka transaction 使用
    private Set<String> generateNewTransactionalIds() {
		checkState(nextTransactionalIdHint != null, "nextTransactionalIdHint must be present for EXACTLY_ONCE");

		Set<String> transactionalIds = transactionalIdsGenerator.generateIdsToUse(nextTransactionalIdHint.nextFreeTransactionalId);
		LOG.info("Generated new transactionalIds {}", transactionalIds);
		return transactionalIds;
	}
    // 产生 poolsize 个 transactionId
    public Set<String> generateIdsToUse(long nextFreeTransactionalId) {
		Set<String> transactionalIds = new HashSet<>();
		for (int i = 0; i < poolSize; i++) {
			long transactionalId = nextFreeTransactionalId + subtaskIndex * poolSize + i;
			transactionalIds.add(generateTransactionalId(transactionalId));
		}
		return transactionalIds;
	}
    private String generateTransactionalId(long transactionalId) {
		return prefix + "-" + transactionalId;
	}

    // 恢复挂起的 Kafka 事务，在 base 的 initializeState 方法中调用。
    @Override
	protected void recoverAndCommit(KafkaTransactionState transaction) {
		if (transaction.isTransactional()) {
			try (
				FlinkKafkaProducer<byte[], byte[]> producer =
					initTransactionalProducer(transaction.transactionalId, false)) {
				producer.resumeTransaction(transaction.producerId, transaction.epoch);
                // 事务恢复之后，提交事务
				producer.commitTransaction();
			} catch (InvalidTxnStateException | ProducerFencedException ex) {
				// That means we have committed this transaction before.
				LOG.warn("Encountered error {} while recovering transaction {}. " +
						"Presumably this transaction has been already committed before",
					ex,
					transaction);
			}
		}
        // 至此，事务恢复完成
	}

    private FlinkKafkaProducer<byte[], byte[]> initTransactionalProducer(String transactionalId, boolean registerMetrics) {
        // 给 新创建的 producer 配置上从 checkpont state 中恢复的事务Id。
        // 这里也从侧面看出，FlinkKafakaProducer 虽然并没有将 KafkaProducer 给序列化之后进行快照状态存储，
        // 但是在每次从 checkpoint Recover 的时候，kafkaProducer 都是新创建的，这也符合 flink state 小状态的设计
        // 同时 kafkaProducer 也配置上了指定的事务 Id，kafkaProducer 就可以对指定的事务进行恢复了
        // 但是这个思路没有办法用到 mysql 身上，我查了资料，可能是 jdbc 的 api 设计的太早，并没有考虑2阶段提交这个玩意
		initTransactionalProducerConfig(producerConfig, transactionalId);
        // 完成 kafkaProducer 的初始化
		return initProducer(registerMetrics);
	}

    	/**
	 * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously obtained ones,
	 * so that we can resume transaction after a restart. Implementation of this method is based on
	 * {@link org.apache.kafka.clients.producer.KafkaProducer#initTransactions}.
     * 反射的方式恢复 kafka 的事务
	 */
	public void resumeTransaction(long producerId, short epoch) {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			Preconditions.checkState(producerId >= 0 && epoch >= 0,
				"Incorrect values for producerId %s and epoch %s",
				producerId,
				epoch);
			LOG.info("Attempting to resume transaction {} with producerId {} and epoch {}",
				transactionalId,
				producerId,
				epoch);
            // 获取 kafkaProducer 对象的 kafkaMamanger 对象
			Object transactionManager = getValue(kafkaProducer, "transactionManager");
			synchronized (transactionManager) {
				Object sequenceNumbers = getValue(transactionManager, "sequenceNumbers");

                //先反射方式将 transactionManager 的 transitionTo 改为 INITIALIZING
				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
                // 清理计数器？
				invoke(sequenceNumbers, "clear");

				Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
                // 给 producerIdAndEpoch 对象设置 producerId 和 epoch（从 checkpoint state 中恢复的）
				setValue(producerIdAndEpoch, "producerId", producerId);
				setValue(producerIdAndEpoch, "epoch", epoch);

                // 再次反射地将 transactionManager 的 transitionTo 设置为 READY
				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));
                // 最后反射地将 transactionManager 的 transitionTo 设置为 IN_TRANSACTION
				invoke(transactionManager,
					"transitionTo",
					getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
                // 最后的最后，设置 transactionManager 的事务状态开启的标志为 true
				setValue(transactionManager, "transactionStarted", true);
			}
		}
	}
    // 方法反射调用的封装
    private static Object invoke(Object object, String methodName, Object... args) {
		Class<?>[] argTypes = new Class[args.length];
		for (int i = 0; i < args.length; i++) {
			argTypes[i] = args[i].getClass();
		}
		return invoke(object, methodName, argTypes, args);
	}

	private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
		try {
			Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
			method.setAccessible(true);
			return method.invoke(object, args);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new RuntimeException("Incompatible KafkaProducer version", e);
		}
	}
    //提交事务
	@Override
	public void commitTransaction() throws ProducerFencedException {
		synchronized (producerClosingLock) {
			ensureNotClosed();
			kafkaProducer.commitTransaction();
		}
	}

    // 初始化完成之后，开启一个 事务
    @Override
	protected KafkaTransactionState beginTransaction() throws FlinkKafka011Exception {
		switch (semantic) {
            // exactly once 则每次 beginTransaciton 都启动一个新的 transaction 
			case EXACTLY_ONCE:
				FlinkKafkaProducer<byte[], byte[]> producer = createTransactionalProducer();
                // 调用 kafkaProducer 的 beginTransaction 方法，启动 Kafka 的 transaction
				producer.beginTransaction();
                // 将 kafak 的事务 id  和 producer 封装后返回
				return new KafkaTransactionState(producer.getTransactionalId(), producer);
			case AT_LEAST_ONCE:
			case NONE:
				// Do not create new producer on each beginTransaction() if it is not necessary
				final KafkaTransactionState currentTransaction = currentTransaction();
				if (currentTransaction != null && currentTransaction.producer != null) {
					return new KafkaTransactionState(currentTransaction.producer);
				}
				return new KafkaTransactionState(initNonTransactionalProducer(true));
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
	}

    // invoke 方法，对 flinkkafkaProducer 来说，就是数据发送到 kafka
    // SinkFunction 的 invoke 回到用该实现方法
    @Override
	public void invoke(KafkaTransactionState transaction, IN next, Context context) throws FlinkKafka011Exception {
		checkErroneous();
        // 这里跟 flinkKafkaProducer010 一样，从 schema 中获取 key, value ,topic
		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		String targetTopic = schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		Long timestamp = null;
		if (this.writeTimestampToKafka) {
			timestamp = context.timestamp();
		}

		ProducerRecord<byte[], byte[]> record;
        // 构造 ProducerRecord
		int[] partitions = topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, transaction.producer);
			topicPartitionsMap.put(targetTopic, partitions);
		}
		if (flinkKafkaPartitioner != null) {
			record = new ProducerRecord<>(
				targetTopic,
				flinkKafkaPartitioner.partition(next, serializedKey, serializedValue, targetTopic, partitions),
				timestamp,
				serializedKey,
				serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
		}
		pendingRecords.incrementAndGet();
        // 发送 kafka record
		transaction.producer.send(record, callback);
	}

    // FlinkKafkaProducer 的 checkpoint 方法
    @Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 先效用 父类的 snapshotState 方法，执行预提交，然后将状态数据存储到 state 中
		super.snapshotState(context);

		nextTransactionalIdHintState.clear();
		// To avoid duplication only first subtask keeps track of next transactional id hint. Otherwise all of the
		// subtasks would write exactly same information.
		if (getRuntimeContext().getIndexOfThisSubtask() == 0 && semantic == Semantic.EXACTLY_ONCE) {
			checkState(nextTransactionalIdHint != null, "nextTransactionalIdHint must be set for EXACTLY_ONCE");
			long nextFreeTransactionalId = nextTransactionalIdHint.nextFreeTransactionalId;

			// If we scaled up, some (unknown) subtask must have created new transactional ids from scratch. In that
			// case we adjust nextFreeTransactionalId by the range of transactionalIds that could be used for this
			// scaling up.
			if (getRuntimeContext().getNumberOfParallelSubtasks() > nextTransactionalIdHint.lastParallelism) {
				nextFreeTransactionalId += getRuntimeContext().getNumberOfParallelSubtasks() * kafkaProducersPoolSize;
			}
            // 这里将下一个事务 ID 的值持久化道 状态中，以便 从 checkpoint 恢复的时候，可以取出其中的 事务Id ，重新提交
			nextTransactionalIdHintState.add(new NextTransactionalIdHint(
				getRuntimeContext().getNumberOfParallelSubtasks(),
				nextFreeTransactionalId));
		}
	}

    // 在 checkpoint 的时候执行预提交，flinkKafkaProducer 的 预提交代码如下：
    @Override
	protected void preCommit(KafkaTransactionState transaction) throws FlinkKafka011Exception {
		switch (semantic) {
			case EXACTLY_ONCE:
			case AT_LEAST_ONCE:
				flush(transaction);
				break;
			case NONE:
				break;
			default:
				throw new UnsupportedOperationException("Not implemented semantic");
		}
		checkErroneous();
	}

    /**
	 * Flush pending records.
     * 调用 KafkaProducer 写入一批数据之后，调用 flush 方法将缓存中的数据也尽快刷新写入 kafka broker 中
	 * @param transaction
	 */
	private void flush(KafkaTransactionState transaction) throws FlinkKafka011Exception {
		if (transaction.producer != null) {
            // 调用 KafkaProducer 的 flush 方法。 因为 Kafka Producer 的提交也是批量的
			transaction.producer.flush();
		}
		long pendingRecordsCount = pendingRecords.get();
		if (pendingRecordsCount != 0) {
			throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecordsCount);
		}

		// if the flushed requests has errors, we should propagate it also and fail the checkpoint
		checkErroneous();
	}

    // 提交事务，在父类的 notifyCheckpointCompleted  回调函数中调用的
    // kafka 的事务提交之后，消费端如果事务隔离级别是 READ_COMMITED ，则才会读到这些提交的数据。
    @Override
	protected void commit(KafkaTransactionState transaction) {
		if (transaction.isTransactional()) {
			try {
				transaction.producer.commitTransaction();
			} finally {
				recycleTransactionalProducer(transaction.producer);
			}
		}
	}

}
```