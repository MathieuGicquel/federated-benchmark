package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.algebra.StatementSource;
import org.eclipse.rdf4j.federated.monitoring.MonitoringUtil;
import org.eclipse.rdf4j.federated.monitoring.QueryPlanLog;
import org.eclipse.rdf4j.federated.optimizer.SourceSelection;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Federapp {
    private static final Logger log = LoggerFactory.getLogger(Federapp.class);
    public static final boolean force_source_selection = true;
    public static final Map<String,Object> CONTAINER = new ConcurrentHashMap<>();
    public static final String SOURCE_SELECTION_KEY = "SOURCE_SELECTION";
    public static final String SOURCE_SELECTION2_KEY = "SOURCE_SELECTION_DO_SOURCE_SELECTION";
    public static final String COUNT_HTTP_REQ_KEY = "HTTPCOUNTER";
    public static final String LIST_HTTP_REQ_KEY = "HTTPLIST";

    public static final String MAP_SS = "MAP_SS";


    public static final String CSV_HEADER = "query,exec_time,nb_source_selection,nb_http_request\n";

    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        // init
        CONTAINER.put(COUNT_HTTP_REQ_KEY,new AtomicInteger());
        CONTAINER.put(LIST_HTTP_REQ_KEY,new ConcurrentLinkedQueue<>());
        String configPath = args[0];
        String queryPath = args[1];
        String resultPath = args[2];
        String statPath = args[3];
        String sourceSelectionPath = args[4];
        String httpListFilePath = args[5];
        String ssPath = args[6];

        BufferedWriter statWriter = new BufferedWriter(new FileWriter(statPath));

        String rawQuery = new String(Files.readAllBytes(Paths.get(queryPath)));
        File dataConfig = new File(configPath);

        Long startTime = null;
        Long endTime = null;

        FedXRepository repo  = FedXFactory.newFederation()
                .withMembers(dataConfig)
                .withConfig(new FedXConfig()
                        .withEnableMonitoring(true)
                        .withLogQueryPlan(true)
                        .withLogQueries(true)
                        .withDebugQueryPlan(true)
                        .withEnforceMaxQueryTime(86400)
                )
                .create();

        try (RepositoryConnection conn = repo.getConnection()) {
            startTime = System.currentTimeMillis();
            TupleQuery tq = conn.prepareTupleQuery(rawQuery);
            try (TupleQueryResult tqRes = tq.evaluate()) {
                endTime = System.currentTimeMillis();
                createResultFile(resultPath, tqRes);
            }

            long durationTime = endTime - startTime;
            statWriter.write(CSV_HEADER);
            int nbSourceSelection =
                    ((SourceSelection)CONTAINER.get(SOURCE_SELECTION_KEY))
                            .getRelevantSources().size();

            int httpqueries = ((AtomicInteger) CONTAINER.get(COUNT_HTTP_REQ_KEY)).get();
            statWriter.write(
                    queryPath + ","
                            + durationTime + ","
                            + nbSourceSelection + ","
                            + httpqueries +
                            "\n");


            createSourceSelectionFile(sourceSelectionPath);
            createHttpListFile(httpListFilePath);
        }catch (Exception e) {
            log.error("An exception occurred!",e);

            statWriter.write(CSV_HEADER);
            statWriter.write(queryPath + "," +"failed,failed,failed" + "\n");

            createSourceSelectionFile(sourceSelectionPath);
            createHttpListFile(httpListFilePath);
            //throw e;
        }


        repo.shutDown();
        statWriter.close();


    }

    private static void createSourceSelectionFile(String sourceSelectionPath) throws Exception {
        BufferedWriter sourceSelectionWriter = new BufferedWriter(new FileWriter(sourceSelectionPath));
        sourceSelectionWriter.write("triple,source_selection\n");
        Map<StatementPattern, List<StatementSource>> stmt = ((Map<StatementPattern, List<StatementSource>>)Federapp.CONTAINER.get(Federapp.SOURCE_SELECTION2_KEY));
        if(stmt != null) {
            for (StatementPattern pattern: stmt.keySet()) {
                sourceSelectionWriter.write(("\"" + pattern + "\"," + "\"" +stmt.get(pattern).toString()).replace("\n"," ") + "\"\n");
            }
        }
        sourceSelectionWriter.close();
    }

    private static void createResultFile(String resultFilePath, TupleQueryResult tq) throws Exception{
        BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(resultFilePath));
        while (tq.hasNext()) {
            BindingSet b = tq.next();
            queryResultWriter.write(b.toString() + "\n");
            System.out.println("RESULT " + b.toString() + "\n");
        }
        queryResultWriter.close();
    }

    private static void createHttpListFile(String path) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));

        Queue<String> q =(Queue) CONTAINER.get(LIST_HTTP_REQ_KEY);
        for (String s: q) {
            writer.write(s + "\n");
        }

        writer.close();
    }

    private static void parseSS(String path) throws Exception {
        String rawSS = new String(Files.readAllBytes(Paths.get(path)));
        String[] tabSS = rawSS.split("\n");
        Map<Integer,Set<String>> tpMap = new ConcurrentHashMap<>();

        int i = 0;
        for (String l : tabSS) {
            if(i>0){
                int j=0;
                for (String tp : l.split(",")) {
                    if(!tpMap.containsKey(j)){
                        tpMap.put(j, new HashSet<>());
                    }
                    String ss = tp.split("g/")[1];
                    tpMap.get(j).add("sparql_example.org_"+ss);
                }
            }
            i++;
        }

        ((Map)CONTAINER.get(MAP_SS)).put(MAP_SS, tpMap);
    }
}
