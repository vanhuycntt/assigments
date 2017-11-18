package com.myapp;

import com.myapp.csv.FileHelper;
import com.myapp.csv.PhoneRecord;
import com.myapp.task.PhoneSeparationTask;
import com.myapp.task.RangeRead;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The class is responsible for finding the real activation date.
 */
public class RealActivationDateFinder {

    private static final ExecutorService tasksPool = Executors.newFixedThreadPool(10);
    /**
     * The constant value of batch size, increase this batch size to process large file,
     * but in the limit of memory
     */
    private static final int BATCH_SIZE_TO_READ = 2;
    /**
     * Number of batches for processing, and each batch is assigned to each task per thread to process
     * Increase this constant to make more tasks handling the large file.
     */
    private static final int NUMBER_OF_BATCH_TO_PROCESS = 3;


    /**
     * The input file to process records
     */
    private String inputPath;

    /**
     * Total records of the input file needs to process, this value is not include the header line file.
     *
     */
    private long totalRecords;

    /**
     *
     */
    private Path separationFolderPath;
    /**
     *
     * @param totalRecords
     * @param inputFilePath
     */
    public RealActivationDateFinder(long totalRecords, String inputFilePath ) throws IOException {
        this.inputPath = inputFilePath;
        this.totalRecords = totalRecords;
        this.separationFolderPath = FileHelper.getSeparationFolderPath();
    }

    /**
     * The primary execution to find real activation date of all phones in the csv file,
     * this execution will do main tasks:
     * - Separate the processed file into chunks of records
     * - Assign chunks of records to sub tasks to process
     * - Aggregate the phone numbers from sub tasks, find the real activation date,
     * and write to output file
     */
    public Path execute() throws IOException{
        //exclude the header line, and process from the second line
        long actualTotalRead = totalRecords + 1;
        RangeRead seedRangeRead = this.totalRecords > BATCH_SIZE_TO_READ ?
                new RangeRead(2, BATCH_SIZE_TO_READ + 2)
                : new RangeRead(2, actualTotalRead );
        //separate the total records into range to read and process
        List<RangeRead> rangeReads = new LinkedList<>();
        Stream.iterate(seedRangeRead,
                rangeRead -> new RangeRead(rangeRead.getExcludeEndPos(),
                        rangeRead.getExcludeEndPos() + BATCH_SIZE_TO_READ))
                .filter(rangeRead -> {
                    boolean isStillLessThan = rangeRead.getExcludeEndPos() <= actualTotalRead;
                    if(isStillLessThan) {
                        rangeReads.add(rangeRead);
                    }
                    return rangeRead.getExcludeEndPos() > actualTotalRead;
                }).findFirst().get();

        RangeRead lastRangeRead = rangeReads.get(rangeReads.size() - 1);
        if(lastRangeRead.getExcludeEndPos() < actualTotalRead) {
            rangeReads.add(new RangeRead(lastRangeRead.getExcludeEndPos(), actualTotalRead));
        }

        AtomicInteger batchNumberIndex = new AtomicInteger(0);
        List<RangeRead> bulkRangeReads = new ArrayList<>();
        Set<String>allPhoneNumberSet = new HashSet<>();
        rangeReads.forEach(rangeRead -> {
            bulkRangeReads.add(rangeRead);
            if(batchNumberIndex.incrementAndGet() == NUMBER_OF_BATCH_TO_PROCESS) {
                allPhoneNumberSet.addAll(processRangeRead(bulkRangeReads));
                batchNumberIndex.set(1);
                bulkRangeReads.clear();
            }
        });
        if(bulkRangeReads.size() > 0) {
            allPhoneNumberSet.addAll(processRangeRead(bulkRangeReads));
        }
        return this.writeResultsToOuput(allPhoneNumberSet);
    }

    /**
     *
     * @param rangeReads
     * @return
     */
    private Set<String> processRangeRead(List<RangeRead> rangeReads) {
        List<CompletableFuture<Set<String>>> phoneSeparationTasks = rangeReads.stream().map(rangeRead -> {
            try {
                Path fileInputPath = FileHelper.getOrCreateFile(this.inputPath);
                List<PhoneRecord> records = FileHelper.readRecords(fileInputPath, rangeRead);
                return CompletableFuture.supplyAsync(() -> {
                    PhoneSeparationTask separatePhoneTask = new PhoneSeparationTask(records, this.separationFolderPath);
                    return separatePhoneTask.execute();
                });
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toList());
        //wait until all phone separation tasks completed
        CompletableFuture.allOf(phoneSeparationTasks.toArray(new CompletableFuture[0]));
        //aggregate results from completed tasks
        return phoneSeparationTasks.stream().map(completedTask -> {
            try {
                return completedTask.get();
            } catch(Exception ex) {
                throw new RuntimeException(ex);
            }
        }).reduce(new HashSet<>(), (Set<String> firstSet, Set<String> secondSet)-> {
            firstSet.addAll(secondSet);
            return firstSet;
        });
    }

    /**
     *
     * @param phoneNumbers
     */
    private Path writeResultsToOuput(Set<String> phoneNumbers) throws IOException {
        Objects.requireNonNull(phoneNumbers);
        return FileHelper.writeResults(writer -> {
            phoneNumbers.parallelStream().forEach(phone -> {
                try {
                    Path phonePath = FileHelper.getOrCreatePhonePath(this.separationFolderPath, phone);
                    Optional<PhoneRecord> resultOpt = this.findActivationDate(phonePath.toString());

                    if(resultOpt.isPresent()) {
                        PhoneRecord record = resultOpt.get();
                        writer.append(record.getPhoneNumber() + "," + record.getActivateDate().toString());
                        writer.newLine();
                    }
                } catch(IOException ex) {
                    ex.printStackTrace();
                }
            });
        });

    }

    /**
     *
     * @param uniquePhoneRecordsPath
     * @return
     * @throws IOException
     */
    private Optional<PhoneRecord> findActivationDate(String uniquePhoneRecordsPath) throws IOException {
        boolean isExisted = FileHelper.isExistedPath(uniquePhoneRecordsPath);
        if(!isExisted) {
            return Optional.empty();
        }
        Path filePath =  FileHelper.getOrCreateFile(uniquePhoneRecordsPath);
        List<PhoneRecord> records = FileHelper.readAllRecords(filePath);

        LinkedList<PhoneRecord>sortedList = records.stream().sorted().collect(Collectors.toCollection(LinkedList::new));

        return sortedList.stream().reduce((firstRecord, secondRecord) -> {
            if(firstRecord.isNotConsecutiveAndGreaterThan(secondRecord)) {
                return firstRecord;
            }
            return secondRecord;
        });
    }

}
