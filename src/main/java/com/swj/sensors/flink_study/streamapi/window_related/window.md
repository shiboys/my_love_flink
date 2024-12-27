# 窗口
___
窗口(window) 是处理无界流的关键所在。窗口可以将数据流装入大小有限的”桶“中，在对每个“桶”加以处理。本章节的重心将放在 Flink 如何进行窗口操作以及开发者如何尽可能利用 Flink 所提供的的功能。

下面展示了 Flink 窗口在 keyed Streams 和 non-keyed Streams 上使用的基本结构。我们可以看到，这两者的唯一区别自安于：keyed streams 要低啊用 keyBy(...)以后再调用 window(...), 而 non-keyed Streams 之用直接调用 windowAll(...)。留意这个区别，他能把我们更好地理解后面的内容。

**Keyed Windows**

```java
stream
       .keyBy(...)               <-  仅 keyed 窗口需要
       .window(...)              <-  必填项："assigner"
      [.trigger(...)]            <-  可选项："trigger" (省略则使用默认 trigger)
      [.evictor(...)]            <-  可选项："evictor" (省略则不使用 evictor)
      [.allowedLateness(...)]    <-  可选项："lateness" (省略则为 0)
      [.sideOutputLateData(...)] <-  可选项："output tag" (省略则不对迟到数据使用 side output)
       .reduce/aggregate/apply()      <-  必填项："function"
      [.getSideOutput(...)]      <-  可选项："output tag"
```

**Non-Keyed Windows**

```java
stream
       .window_all(...)             <-  必填项："assigner"
      [.trigger(...)]               <-  可选项："trigger" (else default trigger)
      [.allowed_lateness(...)]      <-  可选项："lateness" (else zero)
      [.side_output_late_data(...)] <-  可选项："output tag" (else no side output for late data)
       .reduce/aggregate/apply()    <-  必填项："function"
      [.get_side_output(...)]       <-  可选项："output tag"
```

上面方口号中 的 ([...]) 命令时可选的。也就是说，Flink 允许你自定义多样化的窗口操作来满足你的要求

## 窗口的生命周期
___

简单来说，一个窗口在第一个属于它的元素达到时就会被**创建**，然后在时间 (event 或 processing time) 超过窗口的“结束时间戳+用户定义的 allowed lateness” 时被 **完全删除**。Flink 仅保证删除基于时间的窗口，其他类型的窗口不做保证，比如全局窗口。例如，对于一个基于 event time 且范围互不重合（滚动）的窗口策略，如果窗口设置的时长为 5 分钟，可容忍的迟到时间（allowed latency） 为 1 分钟，那么第一个元素落入 12:00 至 12:05 这个区间时，Flink 就会为这个区间创建一个新的窗口。当 watermark 越过 12:06 时，这个窗口将被摧毁。

另外，每个窗口会设置自己的 Trigger 和 function(ProcessWindowFunction、ReduceFunction、或 AggregateFunction)。该 Function 决定如何计算窗口中的内容，而 Trigger 决定何时窗口中的数据可以被 function 计算。Trigger 的触发(fire) 条件可能是 "当窗口中有多余 4 条数据"或 "当 watermark 越过窗口的结束时间" 等。Trigger 还可以在 window 被创建后，被删除前的这段时间内定义何时清理(purge)窗口中的数据。这里的数据仅指窗口中的元素，不包括窗口中的 meta data。也就是说，窗口在 purge 后仍然可用加入新的数据。

除此之外，你也可以指定一个 Evictor, 在 trigger 触发之后，Evictor 可用在窗口函数的前后删除数据（evictor 的 before 和 after 方法）。


## Window Assigner
___

### 会话窗口
会话窗口的 assigner 会把数据按活跃的会话分组。与 *滚动窗口* 和 *滑动窗口* 不同，会话窗口不会相互重叠，且没有固定的开始或结束时间。会话窗口在一段时间没有收到数据之后就会关闭，即在一段不活跃的间隔之后。会话窗口的 assigner 客户设置固定的会话间隔(session gap) 或用 session gap extrator 函数来动态定义多长时间算作不活跃。当超出了不活跃的时间段，当前的会话就会关闭，并且将接下来的数据分发到新的会话窗口。

![session window](../../../../../../resources/imgs/session-windows.svg)

# Triggers
___
Trigger 决定了一个窗口 (由 window assigner 定义)何时可以被 window function 处理。每个 WindowAssigner 都有一个默认的 Trigger。如果默认 trigger 无法满足你的要求，你可以在 trigger(...) 中调用指定自定义的 trigger。

Trigger 接口提供了 5 个方法来响应不同的事件：

* onElement() 方法在每个元素被加入窗口时调用
* onEventTime() 方法在注册的 event-time timer 触发时调用
* onProcessingTime() 方法在注册的 processing-time timer 触发时调用。
* onMerge() 方法与有状态的 trigger 有关。该方法会在两个窗口合并时，将窗口对应 trigger 的状态进行合并，比如使用会话窗口时。
* 最后，clear() 方法处理在对应窗口被移除时所需要的逻辑。

有两点需要注意：

1、前 3 个方法通过返回 TriggerResult 来决定 trigger 如何应对达到窗口的事件。应对的方案有以下几种：
* CONTINUE：什么都不做
* FIRE：触发计算
* PURGE：清空窗口内的元素
* FIRE_AND_PURGE：触发计算，计算结束后清空窗口内的元素

2. 上面任意方法都可以用来注册 processing-time 或 event-time timer。

## 触发 (Fire) 与清除 (Purge)

当 trigger 认定一个窗口可以被计算时，他就会触发，，也就是返回 FIRE 或 FIRE_AND_PURGE。这是让窗口算子发送当前窗口计算结果的信号。如果一个窗口指定了 ProcessWindowFunction, 所有的元素都会传给 ProcessWindowFunction。如果是 ReduceFunction 或 AggregateFunction，则直接发送聚合结果。

当 trigger 触发时，它可以返回 FIRE 或者 FIRE_AND_PURGE。FIRE 会保留被触发窗口中的内容，而 FIRE_AND_PURGE 会删除这些内容。Flink 内置的 trigger 默认使用 Fire，不会清除窗口的状态。

## WindowAssigner 默认的 Triggers

WindowAssigner 默认的 Trigger 足以应付诸多情况。比如说，所有的 event-time window assigner 都默认使用 EventTimeTrigger。这个 trigger 会在 watermark 越过窗口结束时间后直接触发。

GlobalWindow 的默认 trigger 是永远都不会触发的 NeverTrigger。因此，使用 GlobalWindow 时，你必须自定义一个 trigger。

> 当你在 trigger() 中指定一个 trigger 时，你实际上覆盖了当前 WindowAssigner 默认的 trigger。比如说，如果你指定了一个 
> CountTrigger 给 TumblingEventTimeWindows，你的窗口将不再根据时间触发，而是根据元素数量触发。如果你希望即响应时间又响应数量，
> 就需要自定义 trigger 了。

## 内置 Triggers 和 自定义 Triggers

Flink 包含了一些内置 Trigger。
* 之前提到过的 EventTime Trigger 根据 watermark 测量的 event time 触发
* ProcessingTimeTrigger 根据 processing time 触发
* CountTrigger 在窗口中的元素超过预设的限制时触发。
* PurgingTrigger 接收另一个 trigger 并将它转换成一个会清理数据的 trigger。

如果你需要实现自定义的 trigger, 你需要参考 Trigger 这个抽象类。

# Evictors
___

Flink 的窗口模型允许你在 WindowAssigner 和 Trigger 之外指定可选的 Evictor。如本文的开篇代码中所示，通过 evictor(...) 方法传入 Evictor。Evictor 可以在 trigger 触发后，调用窗口函数之前或之后从窗口中删除元素。Evictor 接口提供了两个方法实现此功能：

```java
/**
 * Optionally evicts elements. Called before windowing function.
 *
 * @param elements The elements currently in the pane.
 * @param size The current number of elements in the pane.
 * @param window The {@link Window}
 * @param evictorContext The context for the Evictor
 */
void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

/**
 * Optionally evicts elements. Called after windowing function.
 *
 * @param elements The elements currently in the pane.
 * @param size The current number of elements in the pane.
 * @param window The {@link Window}
 * @param evictorContext The context for the Evictor
 */
void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
```

evicBefore() 包含在调用窗口函数(window function) 之前的函数，而 evictAfter 包含在调用窗口之后的逻辑。在调用窗口函数之前被移除的元素不会被窗口函数计算。

Flink 内置有 3 个 evictor：

* CountEvictor： 仅记录用户指定数量的元素，一旦窗口中的元素数量超过这个数，多余的元素会从窗口缓存的开头移除(有点 LRU 的既视感)
* DeltaEvictor：接收 DeltaFunction 和 threshold 参数，计算最后一个元素与窗口缓存中所有元素的差值，并移除差值大于等于 threshold 的元素
* TimeEvictor：接收 interval 参数，以毫秒表示。它会找到窗口中元素的最大 timestamp max_ts 并移除比 max_ts - interval 小的所有元素

默认情况下，所有内置的 evictor 逻辑都在**调用窗口函数前**执行。

Flink 不对窗口中的元素的做任何保证。也就是说，即使 evictor 从窗口缓存的开头移除一个元素，这个元素也不一定是最先或者最后到达窗口的。
