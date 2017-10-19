package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.TimeZone;

public class AirportTrends {
	
	public enum JFKTerminal {
		TERMINAL_1(71436),
		TERMINAL_2(71688),
		TERMINAL_3(71191),
		TERMINAL_4(70945),
		TERMINAL_5(70190),
		TERMINAL_6(70686),
		NOT_A_TERMINAL(-1);

		int mapGrid;

		private JFKTerminal(int grid){
			this.mapGrid = grid;
		}

		public static JFKTerminal gridToTerminal(int grid){
			for(JFKTerminal terminal : values()){
				if(terminal.mapGrid == grid) return terminal;
			}
			return NOT_A_TERMINAL;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource("nycTaxiRides.gz", 60, 2000));


        DataStream<Tuple3<JFKTerminal, Integer, Integer>> popularTerminal = rides
                .flatMap(new TerminalMapper())
                .keyBy(0)
                .timeWindow(Time.minutes(60))
                .apply(new TerminalCount())
                .timeWindowAll(Time.minutes(60))
                .apply(new PopularCount());

        popularTerminal.print();

		env.execute();

	}

    /**
     * Maps taxi ride to grid cell and event type,
     * filters out rides that do not start or stop at the airport terminals,
     * outputs the terminal connected to the ride
     *
     */
    public static class TerminalMapper implements FlatMapFunction<TaxiRide, Tuple1<JFKTerminal>> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<Tuple1<JFKTerminal>> out) {

            int gridId;

            if(taxiRide.isStart) {
                // get grid cell id for start location
                gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            } else {
                // get grid cell id for end location
                gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            }

            JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
            if (terminal.equals(JFKTerminal.NOT_A_TERMINAL)) return;

            out.collect(new Tuple1<>(terminal));

        }
    }


    /**
     * Counts the number of rides arriving or departing to a terminal
     */
    public static class TerminalCount implements WindowFunction<
            Tuple1<JFKTerminal>, // input type
            Tuple3<JFKTerminal, Integer, Integer>, // output type
            Tuple, // key type
            TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple1<JFKTerminal>> values,
                Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {

            JFKTerminal terminal = ((Tuple1<JFKTerminal>)key).f0;

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone( "America/New_York" ));
            calendar.setTimeInMillis(window.getEnd());
            Integer hour = calendar.get(Calendar.HOUR_OF_DAY);

            int count = 0;
            for(Tuple1<JFKTerminal> v : values) {
                count += 1;
            }

            out.collect(new Tuple3<>(terminal, count, hour));
        }
    }

    /**
     * Determines the terminal with most amount of rides
     */
    public static class PopularCount implements AllWindowFunction<
                Tuple3<JFKTerminal, Integer, Integer>, // input type
                Tuple3<JFKTerminal, Integer, Integer>, // output type
                TimeWindow>
    {
        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                TimeWindow window,
                Iterable<Tuple3<JFKTerminal, Integer, Integer>> values,
                Collector<Tuple3<JFKTerminal, Integer, Integer>> out) {

            int maxValue = 0;
            Tuple3<JFKTerminal, Integer, Integer> t = new Tuple3<>(JFKTerminal.NOT_A_TERMINAL, 0, -1) ;


            for(Tuple3<JFKTerminal, Integer, Integer> v : values) {
                if (v.f1 >= maxValue) {
                    maxValue = v.f1;
                    t = v;
                }
            }

            out.collect(t);
        }
    }
}