package org.example;

import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.monitoring.MonitoringUtil;
import org.eclipse.rdf4j.federated.monitoring.QueryPlanLog;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Federapp {
    public static void main(String[] args) throws Exception {
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



        try (Stream<String> lines = Files.lines(Paths.get(configPath))) {

            // Formatting like \r\n will be lost
            // String content = lines.collect(Collectors.joining());

            // UNIX \n, WIndows \r\n
            String content = lines.collect(Collectors.joining(System.lineSeparator()));
            System.out.println(content);

            // File to List
            //List<String> list = lines.collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }






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
        }
        Long durationTime = endTime - startTime;
        //MonitoringUtil.printMonitoringInformation(repo.getFederationContext());
        //System.out.println("# Optimized Query Plan:");
        //System.out.println(QueryPlanLog.getQueryPlan());
        //System.out.println(repo.getQueryManager().getRunningQueries().size());


        statWriter.write("query,exec_time\n");
        statWriter.write(FilenameUtils.getBaseName(FilenameUtils.getBaseName(queryPath)) + "," +durationTime + "\n");

        repo.shutDown();
        statWriter.close();
        queryResultWriter.close();


    }
}
