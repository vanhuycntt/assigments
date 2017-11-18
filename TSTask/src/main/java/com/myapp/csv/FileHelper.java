package com.myapp.csv;

import com.myapp.task.RangeRead;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileHelper {

    /**
     * Read records from an included start position to an excluded end position.
     * @param rangeRead
     * @return
     * @throws IOException
     */
    public static List<PhoneRecord> readRecords(Path filePath, RangeRead rangeRead) throws IOException {
        return Files.lines(filePath)
                .skip(rangeRead.getIncludeStartPos() - 1)
                .limit(rangeRead.getExcludeEndPos() - rangeRead.getIncludeStartPos())
                .map(FileHelper::convertToRecord).collect(
                        Collectors.toCollection(LinkedList::new)
                );

    }

    /**
     * Read all lines in file and convert them to phone records
     * @param filePath
     * @return
     * @throws IOException
     */
    public static List<PhoneRecord> readAllRecords(Path filePath) throws IOException {
        return Files.lines(filePath).map(FileHelper::convertToRecord).collect(Collectors.toCollection(LinkedList::new));
    }

    private static PhoneRecord convertToRecord(String line) {
        List<String> rows = Pattern.compile(",").splitAsStream(line).collect(Collectors.toList());
        //required fields
        String phoneNumber = rows.get(0).trim();
        LocalDate activateDate = LocalDate.parse(rows.get(1).trim(), DateTimeFormatter.ISO_LOCAL_DATE);

        LocalDate deactivateDate = null;
        if(rows.size() > 2) {
            deactivateDate = !rows.get(2).trim().equals("") ?
                    LocalDate.parse(rows.get(2).trim(), DateTimeFormatter.ISO_LOCAL_DATE): null;
        }
        if(Objects.isNull(deactivateDate)) {
            return new PhoneRecord(phoneNumber, activateDate);
        }
        return new PhoneRecord(phoneNumber, activateDate, deactivateDate);
    }
    /**
     * Write records by appending to the end of position.
     * @param filePath
     * @param records
     * @throws IOException
     */
    public static synchronized void writeRecords(Path filePath, List<PhoneRecord>records) throws IOException{

        Function<PhoneRecord, String> toLine = (record) -> {
            StringBuilder builder = new StringBuilder();
            builder.append(record.getPhoneNumber());
            builder.append(",");
            builder.append(record.getActivateDate().toString());
            builder.append(",");
            if(record.getDeactivateDate().isPresent()) {
                builder.append(record.getDeactivateDate().get().toString());
            }
            return builder.toString();
        };

        List<String> lines = records
                .stream()
                .map(toLine)
                .collect(Collectors.toCollection(LinkedList::new));

        Files.write(filePath, lines, StandardOpenOption.APPEND);
    }

    /**
     * Get if the file existed or create new file on the path
     * @param filePathStr
     * @return
     * @throws IOException
     */
    public static Path getOrCreateFile(String filePathStr) throws IOException{
        Path filePath = Paths.get(filePathStr);
        if(Files.exists(filePath)) {
            return filePath;
        }
        return Files.createFile(filePath);
    }

    public static  Path writeResults(Consumer<BufferedWriter> writerConsumer) throws IOException{
        Path resultFilePath = FileHelper.getOrCreateFile(System.getProperty("java.io.tmpdir")
                + File.separator + "Result_" + Clock.systemDefaultZone().millis() + ".csv");
        OutputStream out = Files.newOutputStream(resultFilePath, StandardOpenOption.APPEND);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, Charset.forName("UTF-8")))) {
            writer.append("PHONE_NUMBER,REAL_ACTIVATION_DATE");
            writer.newLine();

            writerConsumer.accept(writer);
        }
        return resultFilePath;

    }
    /**
     *
     * @param folderPathStr
     * @return
     * @throws IOException
     */
    public static Path getOrCreateFolder(String folderPathStr) throws IOException{
        Path folderPath = Paths.get(folderPathStr);
        if(Files.exists(folderPath)) {
            return folderPath;
        }
        return Files.createDirectories(folderPath);
    }

    /**
     *
     * @param filePathStr
     * @return
     * @throws IOException
     */
    public static boolean isExistedPath(String filePathStr) throws IOException {
        return Files.exists(Paths.get(filePathStr));
    }
    /**
     *
     * @return
     * @throws IOException
     */
    public static Path getSeparationFolderPath() throws IOException {
        return FileHelper.getOrCreateFolder(System.getProperty("java.io.tmpdir")
                + File.separator + "PhoneSeparator_" + Clock.systemDefaultZone().millis());
    }

    /**
     * Get or create a phone number file path in which is under the separation folder
     * @param separationFolderPath
     * @param phoneNumber
     * @return
     * @throws IOException
     */
    public static synchronized Path getOrCreatePhonePath(Path separationFolderPath, String phoneNumber) throws IOException{
        String filePathStr = separationFolderPath.toString() + File.separator + phoneNumber + ".csv";
        return FileHelper.getOrCreateFile(filePathStr);
    }
}
