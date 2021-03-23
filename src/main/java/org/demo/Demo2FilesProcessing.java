package org.demo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class Demo2FilesProcessing {

    private static final int DATA_IDX = 2;
    private static final int TIPO_IDX = 4;

    private static final String URL_TEMPLATE = "https://raw.githubusercontent.com/sjcdigital/urbam-deaths-data/master/%d/csv/%s.csv";
    private static final List<Integer> ANOS = List.of(2016, 2017, 2018, 2019, 2020, 2021);
    private static final List<String> MESES = List.of("janeiro",
                                                      "fevereiro",
                                                      "marco",
                                                      "abril",
                                                      "maio",
                                                      "junho",
                                                      "julho",
                                                      "agosto",
                                                      "setembro",
                                                      "outubro",
                                                      "novembro",
                                                      "dezembro");

    private static final HttpClient CLIENT = HttpClient.newHttpClient();

    record Registro(String month, String year, String tipo) {
    }

    private static final boolean ASYNC = true;

    public static void main(String[] args) throws Exception {
        var urls = ANOS.stream().flatMap(a -> MESES.stream().map(m -> URL_TEMPLATE.formatted(a, m))).toList();
        var init = System.currentTimeMillis();
        var result = ASYNC ? processaLoom(urls) : processaSequencial(urls);
        System.out.printf("Processados %d registros em %d milisegundos.\n", result.size(), (System.currentTimeMillis() - init));
    }

    private static List<Registro> processaSequencial(List<String> urls) {
        return urls.stream()
                   .map(Demo2FilesProcessing::fetch)
                   .flatMap(Demo2FilesProcessing::processa).toList();
    }

    private static List<Registro> processaLoom(List<String> urls) {
        try (var executor = Executors.newVirtualThreadExecutor()) {
            var tasks = urls.stream()
                            .map(Demo2FilesProcessing::tarefaParaBaixar)
                            .toList();
            return executor.submit(tasks)
                           .map(f -> f.join())
                           .flatMap(Demo2FilesProcessing::processa)
                           .toList();
        }
        
    }

    private static Callable<String> tarefaParaBaixar(String url) {
        return () -> fetch(url);
    }

    private static Stream<Registro> processa(String conteudo) {
        return Optional.ofNullable(conteudo)
                       .stream()
                       .filter(c -> !conteudo.isBlank())
                       .flatMap(String::lines)
                       .skip(1)
                       .map(Demo2FilesProcessing::linhaParaRegistro);
    }

    private static Registro linhaParaRegistro(String line) {
        var parts = line.replaceAll("\"", "").split(",");
        var dateParts = parts[DATA_IDX].split("/");
        var funeral = "DIRETO".equalsIgnoreCase(parts[TIPO_IDX]) ? "DIRETO" : "-";
        return new Registro(dateParts[1], dateParts[2], funeral);
    }

    private static String fetch(String url) {
        System.out.println("Baixando " + url);
        try {
            return CLIENT.send(HttpRequest.newBuilder(URI.create(url)).build(),
                               BodyHandlers.ofString())
                         .body();
        } catch (Exception e) {
            System.err.println("Erro baixando " + url);
            return "";
        }
    }

}