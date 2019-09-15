package driven_case;

import com.alibaba.fastjson.JSONObject;
import model.SessionInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class SessionKeydwProcessFuction {

    public static void main(String[] args) throws Exception {
        /**
         * 初始化环境env
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /**
         * kafka的配置
         */
        String topic = "test2";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topic,new SimpleStringSchema(), prop);

        /**
         * 初始化读取kafka的实时流
         */

        DataStream<Tuple2<String,String>> text =env.addSource(consumer)
                .map(new MapFunction<String,Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String,String> map(String Str) {

                        /**
                         * 回用户session_id,用户事件event_id
                         */
                        JSONObject rowData = JSONObject.parseObject(Str);
                        return new Tuple2(rowData.get("session_id").toString(),rowData.get("event_id").toString());
                    }
                }
        ).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, String>>() {

            @Override
            public Watermark checkAndGetNextWatermark(Tuple2<String, String> stringStringTuple2, long extractTimestamp) {
                return new Watermark(extractTimestamp-1000);
            }

            @Override
            public long extractTimestamp(Tuple2<String, String> stringStringTuple2, long l) {
                return System.currentTimeMillis();
            }
        }).keyBy(0).process(new SessionIdTimeoutFunction()).setParallelism(1);


        text.print().setParallelism(1);
        env.execute();

    }


    }

    /**
    * 1、由于是按key聚合，创建每个key的状态 key=session_id
    * 2、实现KeyedProcessFunction内的onTime方法
    */

class SessionIdTimeoutFunction  extends   KeyedProcessFunction<Tuple,Tuple2<String,String>,Tuple2<String,String>>
    {

    private ValueState<SessionInfo> state;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig config =  StateTtlConfig.newBuilder(Time.minutes(5))
         .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
         .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
         .build();

        ValueStateDescriptor valueStateDescriptor =
                new ValueStateDescriptor("myState1", SessionInfo.class.getClass());

        valueStateDescriptor.enableTimeToLive(config);
        state = getRuntimeContext().getState(valueStateDescriptor);

    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        SessionInfo sessionInfo = new SessionInfo();
        /**
         * 用户sessionid用户行为轨迹
         */
        if(state.value()==null){
            Long timestamp = ctx.timestamp();
            /**
             * 1、输出当前实时流事件，这次没有考虑事件先后顺序
             * 2、如果要对事件先后顺序加一下限制，state需要重新设计
             * 3、这次就简单实现一下原理，没有针对顺序的代码
             */
            out.collect(value);
            /**
             * 如果状体是A,设置下次回调的时间。5秒之后回调
             */
            if(value.f1.equals("A")){
                ctx.timerService().registerEventTimeTimer(timestamp+5000);
                sessionInfo.setEvent_id(value.f1);
                sessionInfo.setSession_id(value.f0);
                sessionInfo.setTimestamp(timestamp);
                state.update(sessionInfo);
            }
        }
        /**
         * 如果发现当前sessionid下有B行为，就更新B
         */
        System.out.println("当前时间:"+sdf.format(new Date(ctx.timestamp())));
        if(value.f1.equals("B")){
            sessionInfo.setEvent_id(value.f1);
            sessionInfo.setSession_id(value.f0);
            sessionInfo.setTimestamp(ctx.timestamp());
            state.update(sessionInfo);
        }

    }

    @Override
     public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {

        /**
         * 如果当前key，5秒之后，没有触发B事件，并且事件一定到了触发的事件点，就输出C事件
         */
        System.out.println("onTimer触发时间：状态记录的时间_触发时间"+sdf.format(new Date(state.value().getTimestamp()))+"_"+sdf.format(new Date(timestamp)));
        if(state.value().getEvent_id()!="B"&&state.value().getTimestamp()+5000==timestamp){
            out.collect(new Tuple2("SessionID为："+ state.value().getSession_id(),"由于5s内没有看到B触发C时间"));

        }

    }

}