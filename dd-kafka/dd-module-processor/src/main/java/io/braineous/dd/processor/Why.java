package io.braineous.dd.processor;

public class Why {

    private String reason;

    private String details;

    public Why(String reason, String details) {
        this.reason = reason;
        this.details = details;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String toString() {
        return "Why{" +
                "reason='" + reason + '\'' +
                ", details='" + details + '\'' +
                '}';
    }
}
