package com.myapp.task;

import com.myapp.csv.FileHelper;
import com.myapp.csv.PhoneRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This is a sub task which does group the same phone number, sorting in descending order,
 * aggregate consecutive records, and finally create a phone number file to write the result
 * for the next processing
 */
public class PhoneSeparationTask {
    /**
     *
     */
    private List<PhoneRecord> records;
    /**
     *
     */
    private Path separationFolderPath;
    /**
     *
     * @param records
     */
    public PhoneSeparationTask(List<PhoneRecord> records, Path separationFolderPath) {
        this.records = records;
        this.separationFolderPath = separationFolderPath;
    }

    /**
     *
     * @return
     */
    public Set<String> execute() {

        //group on the phone number to separate records
        Map<String, List<PhoneRecord>> uniquePhoneNumberMap = this.records.stream().collect(
                Collectors.groupingBy(rc -> rc.getPhoneNumber())
        );
        uniquePhoneNumberMap.forEach((phoneNum, recordValues) -> {
            //sort records to descending order
            List<PhoneRecord> afterSortRecords = recordValues
                    .stream()
                    .sorted()
                    .collect(
                            Collectors.toList()
                    );
            //merge consecutive records to reduce the list
            Map<String, LinkedList<PhoneRecord>> mergeRecords = new HashMap<>();
            afterSortRecords.forEach(rc -> {
                //initial the linked list with one element
                LinkedList<PhoneRecord> initialLinkedList = new LinkedList();
                initialLinkedList.add(rc);
                //using merge function from the map to reducing the consecutive elements in which
                //they are sorted at the descending order
                mergeRecords.merge(rc.getPhoneNumber(), initialLinkedList, (oldList, newList) -> {
                    //get the first element from the new list, the new list always has one elements
                    PhoneRecord newRecord = newList.peek();
                    //the old list is a accumulator containing all elements after reducing.
                    PhoneRecord oldRecord = oldList.peek();
                    if (oldRecord.isConsecutiveAndGreaterThan(newRecord)) {
                        oldList.pop();
                        oldList.push(new PhoneRecord(oldRecord.getPhoneNumber(),
                                newRecord.getActivateDate(), oldRecord.getDeactivateDate()));
                        return oldList;
                    }
                    oldList.push(newRecord);
                    return oldList;
                });
            });
            List<PhoneRecord> afterReducing = mergeRecords.getOrDefault(phoneNum, new LinkedList<>());
            try {
                Path filePath = FileHelper.getOrCreatePhonePath(this.separationFolderPath, phoneNum);
                FileHelper.writeRecords(filePath, afterReducing);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        return uniquePhoneNumberMap.keySet();
    }

}
