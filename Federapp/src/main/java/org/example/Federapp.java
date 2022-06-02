package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.monitoring.MonitoringUtil;
import org.eclipse.rdf4j.federated.monitoring.QueryPlanLog;
import org.eclipse.rdf4j.federated.optimizer.SourceSelection;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Federapp {
    public static final Map<String,Object> CONTAINER = new HashMap<>();
    public static final String SOURCE_SELECTION_KEY = "SOURCE_SELECTION";
    public static final String COUNT_HTTP_REQ_KEY = "HTTPCOUNTER";
    public static final String CSV_HEADER = "query,exec_time,nb_source_selection,nb_http_request\n";

    public static void main(String[] args) throws Exception {
        // init
        CONTAINER.put(COUNT_HTTP_REQ_KEY,new AtomicInteger());
        String configPath = args[0];
        String queryPath = args[1];
        String resultPath = args[2];
        String statPath = args[3];
        System.out.println(Arrays.toString(args));

        BufferedWriter queryResultWriter = new BufferedWriter(new FileWriter(resultPath));
        BufferedWriter statWriter = new BufferedWriter(new FileWriter(statPath));
        String query = new String(Files.readAllBytes(Paths.get(queryPath)));
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
                )
                .create();

        try (RepositoryConnection conn = repo.getConnection()) {
            startTime = System.currentTimeMillis();
            TupleQuery tq = conn.prepareTupleQuery(query);
            try (TupleQueryResult tqRes = tq.evaluate()) {
                endTime = System.currentTimeMillis();
                while (tqRes.hasNext()) {
                    BindingSet b = tqRes.next();
                    queryResultWriter.write(b.toString() + "\n");
                }
            }

            long durationTime = endTime - startTime;
            statWriter.write(CSV_HEADER);
            int nb_source_selection =
                    ((SourceSelection)CONTAINER.get(SOURCE_SELECTION_KEY))
                            .getRelevantSources().size();

            int httpqueries = ((AtomicInteger) CONTAINER.get(COUNT_HTTP_REQ_KEY)).get();
            statWriter.write(
                      queryPath + ","
                        + durationTime + ","
                        + nb_source_selection + ","
                        + httpqueries +
                    "\n");


        }catch (Exception e) {
            queryResultWriter.write("failed\n");
            statWriter.write(CSV_HEADER);
            statWriter.write(queryPath + "," +"failed,failed,failed" + "\n");
        }

        //MonitoringUtil.printMonitoringInformation(repo.getFederationContext());
        //System.out.println("# Optimized Query Plan:");
        //System.out.println(QueryPlanLog.getQueryPlan());
        //System.out.println(repo.getQueryManager().getRunningQueries().size());

        repo.shutDown();
        statWriter.close();
        queryResultWriter.close();


    }
}
