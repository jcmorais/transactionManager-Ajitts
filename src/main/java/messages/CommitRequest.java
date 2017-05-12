package messages;

import hbase.CellId;

import java.io.Serializable;
import java.util.*;

/**
 * Created by carlosmorais on 19/04/2017.
 */
public class CommitRequest implements Serializable, MessageEvent {
    private long id; //commitTS
    private String eventId;
    //private Set<? extends CellId> cells;
    private List<Long> cellId = new ArrayList<>();

    public CommitRequest(long id, Set<? extends CellId> cells) {
        this.id = id;
        this.eventId = UUID.randomUUID().toString();
        for (CellId cell : cells) {
            System.out.println(cell);
            System.out.println(cell.getCellId());
            cellId.add(cell.getCellId());
        }
    }

    public long getId() {
        return id;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public List<Long> getCellId() {
        return cellId;
    }

    public void setCellId(List<Long> cellId) {
        this.cellId = cellId;
    }


    /*
    public Set<? extends CellId> getCells() {
        return cells;
    }

    public void setCells(Set<? extends CellId> cells) {
        this.cells = cells;
    }
    */

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public String toString() {
        return "CommitRequest{" +
                "id=" + id +
                ", eventId='" + eventId + '\'' +
                '}';
    }
}
