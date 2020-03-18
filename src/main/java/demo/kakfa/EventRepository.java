package demo.kakfa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EventRepository extends JpaRepository<EventEntity,String> {
    @Modifying
    @Query(value = "INSERT INTO EVENTS (ID,NAME,TOTAL_SALE_PRICE) VALUES (:id, :name, :price)",nativeQuery = true)
    public void insertEvent(@Param("id") String id, @Param("name")String name, @Param("price") Long totalPrice);

}
