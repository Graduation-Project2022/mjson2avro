package m.json2avro;

public class Model {

    private long end_datetime;
    private String device_id;
    private float reading;

    public Model() {
    }

    public Model(long end_datetime, String device_id, float reading) {
        this.end_datetime = end_datetime;
        this.device_id = device_id;
        this.reading = reading;
    }

    public long getEnd_datetime() {
        return end_datetime;
    }

    public void setEnd_datetime(long end_datetime) {
        this.end_datetime = end_datetime;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public float getReading() {
        return reading;
    }

    public void setReading(float reading) {
        this.reading = reading;
    }
}

