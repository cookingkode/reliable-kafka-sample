package demo.kakfa;

import javax.persistence.*;

@Entity
@Table(name = "events")
public class EventEntity {
    @Column(name = "ID", unique=true)
    //Its important to note unique=true  will only work if you let JPA create your table - which is fine here
    @Id
    private String id;

    private String name;
    private Long totalSalePrice;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTotalSalePrice() {
        return totalSalePrice;
    }

    public void setTotalSalePrice(Long totalSalePrice) {
        this.totalSalePrice = totalSalePrice;
    }

    public  EventEntity(String id, Event e) {
        this.id = id;
        this.name = e.getName();
        this.totalSalePrice = e.getPrice() + e.getTax();
    }

    public EventEntity() {
    }
}
