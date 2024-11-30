package org.dspbench.applications.smartgrid;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dspbench.base.source.generator.Generator;
import org.dspbench.core.Values;
import org.dspbench.applications.smartgrid.model.House;
import org.dspbench.applications.smartgrid.model.Household;
import org.dspbench.applications.smartgrid.model.SmartPlug;
import org.dspbench.utils.RandomUtil;
import org.dspbench.utils.Configuration;
import org.dspbench.base.source.GeneratorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generates smart plug readings at fixed time intervals, storing them
 * into a queue that will be consumed by a {@link GeneratorSource}.
 *
 * The readings are generated by a separated thread and the interval resolutions is
 * of seconds. In order to increase the volume of readings you can decrease the
 * interval down to 1 second. If you need more data volume you will have to tune
 * the other configuration parameters.
 *
 * Configurations parameters:
 *
 *  - {@link SmartGridConstants.Config#GENERATOR_INTERVAL_SECONDS}: interval of record generation in seconds.
 *  - {@link SmartGridConstants.Config#GENERATOR_NUM_HOUSES}: number of houses in the scenario.
 *  - {@link SmartGridConstants.Config#GENERATOR_HOUSEHOLDS_MIN} and {@link SmartGridConstants.Config#GENERATOR_HOUSEHOLDS_MAX}: the range of number of households within a house.
 *  - {@link SmartGridConstants.Config#GENERATOR_PLUGS_MIN} and {@link SmartGridConstants.Config#GENERATOR_PLUGS_MAX}: the range of smart plugs within a household.
 *  - {@link SmartGridConstants.Config#GENERATOR_LOADS}: a comma-separated list of peak loads that will be randomly assigned to smart plugs.
 *  - {@link SmartGridConstants.Config#GENERATOR_LOAD_OSCILLATION}: by how much the peak load of the smart plug will oscillate.
 *  - {@link SmartGridConstants.Config#GENERATOR_PROBABILITY_ON}: the probability of the smart plug being on.
 *  - {@link SmartGridConstants.Config#GENERATOR_ON_LENGTHS}: a comma-separated list of lengths of time to be selected from to set the amount of time that the smart plug will be on.
 *
 *
 * From: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html
 *
 * Generates a dataset of a random set of smart plugs, each being part of a household,
 * which is, in turn, part of a house. Each smart plug records the actual load
 * (in Watts) at each second. The generated dataset is inspired by the DEBS 2014
 * challenge and follow a similar format, a sequence of 6 comma separated values
 * for each line (i.e., for each reading):
 *
 *  - a unique identifier of the measurement [64 bit unsigned integer value]
 *  - a timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [64 bit unsigned integer value]
 *  - a unique identifier (within a household) of the smart plug [32 bit unsigned integer value]
 *  - a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
 *  - a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]
 *  - the measurement [32 bit unsigned integer]
 *
 * @author Alessandro Sivieri
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SmartPlugGenerator extends Generator implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SmartPlugGenerator.class);

    // default config values
    private static final int HOUSES            = 10;
    private static final int HOUSEHOLDS_MIN    = 2;
    private static final int HOUSEHOLD_MAX     = 10;
    private static final int PLUGS_MIN         = 5;
    private static final int PLUGS_MAX         = 20;
    private static final int LOADS[]           = {25, 35, 50, 70, 150, 300, 600, 800, 900, 1200};
    private static final int OSCILLATION       = 10;
    private static final double PROBABILITY_ON = 0.5D;
    private static final int LENGTHS[]         = {600, 900, 1800, 3600, 5400, 7200, 0x1a5e0};

    private long currentId = 0l;

    private int interval;
    private int numHouses;
    private int householdMin;
    private int householdMax;
    private int plugsMin;
    private int plugsMax;
    private int[] loads;
    private int loadOscillation;
    private double probabilityOn;
    private int[] onLenghts;

    private HashMap<Integer, House> houses;
    private ScheduledExecutorService scheduler;
    private BlockingQueue<Values> queue;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);

        interval        = config.getInt(SmartGridConstants.Config.GENERATOR_INTERVAL_SECONDS, 1);
        numHouses       = config.getInt(SmartGridConstants.Config.GENERATOR_NUM_HOUSES, HOUSES);
        householdMin    = config.getInt(SmartGridConstants.Config.GENERATOR_HOUSEHOLDS_MIN, HOUSEHOLDS_MIN);
        householdMax    = config.getInt(SmartGridConstants.Config.GENERATOR_HOUSEHOLDS_MAX, HOUSEHOLD_MAX);
        plugsMin        = config.getInt(SmartGridConstants.Config.GENERATOR_PLUGS_MIN, PLUGS_MIN);
        plugsMax        = config.getInt(SmartGridConstants.Config.GENERATOR_PLUGS_MAX, PLUGS_MAX);
        loads           = config.getIntArray(SmartGridConstants.Config.GENERATOR_LOADS, LOADS);
        loadOscillation = config.getInt(SmartGridConstants.Config.GENERATOR_LOAD_OSCILLATION, OSCILLATION);
        probabilityOn   = config.getDouble(SmartGridConstants.Config.GENERATOR_PROBABILITY_ON, PROBABILITY_ON);
        onLenghts       = config.getIntArray(SmartGridConstants.Config.GENERATOR_ON_LENGTHS, LENGTHS);

        houses    = new HashMap<>(numHouses);
        queue     = new LinkedBlockingQueue<>();
        scheduler = Executors.newScheduledThreadPool(1);

        this.createScenario();
        scheduler.scheduleAtFixedRate(this, 0, interval, TimeUnit.SECONDS);
    }

    /**
     * Create {@link #numHouses} house instances, each with a random number of
     * households between {@link #householdMin} and {@link #householdMax}. Each
     * household in its turn will have a random number of smart plugs between
     * {@link #plugsMin} and {@link #plugsMax}.
     */
    private void createScenario() {
        for (int i = 0; i < numHouses; ++i) {
            House house = new House(i);
            for (int j = 0; j < RandomUtil.randomMinMax(householdMin, householdMax); ++j) {
                Household household = new Household(j);
                for (int k = 0; k < RandomUtil.randomMinMax(plugsMin, plugsMax); ++k) {
                    SmartPlug sp = new SmartPlug(k, loads[RandomUtil.randomMinMax(0, loads.length - 1)],
                            loadOscillation, probabilityOn, onLenghts);
                    household.addSmartPlug(sp);
                }
                house.addHousehold(household);
            }
            houses.put(i, house);
        }
    }

    @Override
    public void run() {
        long ts = (System.currentTimeMillis() / 1000) - interval;

        for (House house : houses.values()) {
            for (Household household : house.getHouseholds()) {
                for (SmartPlug plug : household.getPlugs()) {
                    plug.tryToSetOn(ts);

                    try {
                        queue.put(new Values(
                                String.valueOf(nextId()),
                                ts,
                                plug.getLoad(),
                                SmartGridConstants.Measurement.LOAD,
                                String.valueOf(plug.getId()),
                                String.valueOf(household.getId()),
                                String.valueOf(house.getId())
                        ));

                        queue.put(new Values(
                                String.valueOf(nextId()),
                                ts,
                                plug.getTotalLoadkWh(),
                                SmartGridConstants.Measurement.WORK,
                                String.valueOf(plug.getId()),
                                String.valueOf(household.getId()),
                                String.valueOf(house.getId())
                        ));
                    } catch (InterruptedException ex) {
                        LOG.error("Something wrong happened while waiting to put record in the queue", ex);
                    }
                }
            }
        }

        LOG.info("Finished generation of smart plug logs");
    }

    @Override
    public Values generate() {
        try {
            return queue.take();
        } catch (InterruptedException ex) {
            LOG.error("Unable to get record from queue", ex);
        }
        return null;
    }

    private long nextId() {
        return currentId++;
    }
}
