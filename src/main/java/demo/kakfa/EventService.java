package demo.kakfa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import javax.persistence.PersistenceException;
import javax.transaction.Transactional;
import javax.validation.ConstraintViolationException;

@Service
public class EventService {
    @Autowired
    private EventRepository eventRepository;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

    @Transactional
    public boolean addEvent(String id, Event event) {
        EventEntity eventForDB = new EventEntity(id, event);
        try{
            this.eventRepository.insertEvent(eventForDB.getId(), eventForDB.getName(), eventForDB.getTotalSalePrice());
        }catch(DataIntegrityViolationException e){
            LOGGER.info("Duplicate Event! {} ", e.getMessage());
            return false;
        }
        catch (Exception e){
            LOGGER.error(e.getMessage());
            return false;
        }

        return true;
    }

}
