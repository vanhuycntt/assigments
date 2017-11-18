package com.myapp.csv;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Represent a record in csv file with format like: PHONE_NUMBER,ACTIVATION_DATE,DEACTIVATION_DATE
 */
public class PhoneRecord implements Comparable {

    /**
     * Phone number field
     */
    private String phoneNumber;
    /**
     * Activation date field
     */
    private LocalDate activateDate;
    /**
     * Deactivation date field, this is option field
     */
    private Optional<LocalDate> deactivateDate;

    public PhoneRecord(String phoneNumber, LocalDate activateDate, LocalDate deactivateDate) {
        this.phoneNumber = phoneNumber;
        this.activateDate = activateDate;
        this.deactivateDate = Optional.of(deactivateDate);
    }
    public PhoneRecord(String phoneNumber, LocalDate activateDate, Optional<LocalDate>deactivateDateOpt) {
        this.phoneNumber = phoneNumber;
        this.activateDate = activateDate;
        this.deactivateDate = deactivateDateOpt;
    }

    public PhoneRecord(String phoneNumber, LocalDate activateDate) {
        this.phoneNumber = phoneNumber;
        this.activateDate = activateDate;
        this.deactivateDate = Optional.empty();
    }


    public String getPhoneNumber() {
        return phoneNumber;
    }

    public LocalDate getActivateDate() {
        return activateDate;
    }

    public Optional<LocalDate> getDeactivateDate() {
        return deactivateDate;
    }

    /**
     * Compare the record with the same phone number in descending order.
     * @param that
     * @return
     */
    @Override
    public int compareTo(Object that) {
        PhoneRecord other = (PhoneRecord) that;
        Optional<LocalDate> deactivateDateOpt = this.getDeactivateDate();
        if (deactivateDateOpt.isPresent()) {
            LocalDate deactivateDate = deactivateDateOpt.get();
            if (deactivateDate.isEqual(other.getActivateDate()) || deactivateDate.isBefore(other.getActivateDate())) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PhoneRecord record = (PhoneRecord) o;

        if (getPhoneNumber() != null ? !getPhoneNumber().equals(record.getPhoneNumber()) : record.getPhoneNumber() != null)
            return false;
        if (getActivateDate() != null ? !getActivateDate().isEqual(record.getActivateDate()) : record.getActivateDate() != null)
            return false;
        if(getDeactivateDate() != null && record.getDeactivateDate() != null) {
            if(getDeactivateDate().isPresent() && record.getDeactivateDate().isPresent()) {
                if(getDeactivateDate().get().isEqual(record.getDeactivateDate().get())){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check the record  whether or not the consecutive greater than other record.
     * @param other
     * @return boolean
     */
    public boolean isConsecutiveAndGreaterThan(PhoneRecord other) {
        if(other.getDeactivateDate().isPresent()) {
            if(this.getActivateDate().isEqual(other.getDeactivateDate().get())){
                return true;
            }
        }
        return false;
    }
    /**
     * Check the record whether or not the not consecutive and greater than other record.
     * @param other
     * @return
     */
    public boolean isNotConsecutiveAndGreaterThan(PhoneRecord other) {
        if(other.getDeactivateDate().isPresent()) {
            if(this.getActivateDate().isAfter(other.getDeactivateDate().get())) {
                return true;
            }
        }
        return false;
    }
    @Override
    public int hashCode() {
        int result = getPhoneNumber() != null ? getPhoneNumber().hashCode() : 0;
        result = 31 * result + (getActivateDate() != null ? getActivateDate().hashCode() : 0);
        result = 31 * result + (getDeactivateDate() != null ? getDeactivateDate().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String deactivateDateStr = deactivateDate.isPresent() ? deactivateDate.get().toString() : "";
        return "PhoneRecord{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", activateDate=" + activateDate +
                ", deactivateDate=" + deactivateDateStr +
                '}';
    }
}
