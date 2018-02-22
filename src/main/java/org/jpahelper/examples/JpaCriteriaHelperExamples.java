package org.jpahelper.examples;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.jpahelper.JpaCriteriaHelper;
import org.jpahelper.JpaCriteriaHelper.ComparatorOperator;

public class JpaCriteriaHelperExamples {

    @PersistenceContext
    private EntityManager em;
    
    
    public List<MyEntity> getListOfTop10Ids() {
        return JpaCriteriaHelper
                .select(em, MyEntity.class)
                .where("id", ComparatorOperator.LESS_THAN, 11)
                .orderBy("id")
                .getResults();
    }
    
    public List<MyEntity> getpaginatedListWithMatchingNameLike( String name, Integer pageNumber ) {
        return JpaCriteriaHelper
                .select(em, MyEntity.class)
                .where("name", ComparatorOperator.LIKE_IGNORE_CASE, name)
                .orderBy("name").desc()
                .setPageSize(20)
                .page(pageNumber)
                .getResults();
    }
    
    public boolean existsEntity( Long id, String name, String age ) {
        return JpaCriteriaHelper
                .select(em, MyEntity.class)
                .where("id", id)
                .and("name", name)
                .and("age", age)
                .exists();
    }
    
    public long countEntitiesWithSomeName( String name1, String name2, String name3 ) {
        return JpaCriteriaHelper
                .select(em, MyEntity.class)
                .where("name", name1)
                .or("name", name2)
                .or("name", name3)
                .count();
    }
    
    public MyEntity getAnyOfIds( List<Long> possibleIds ) {
        return JpaCriteriaHelper
                .select(em, MyEntity.class)
                .where("id", ComparatorOperator.IN, possibleIds)
                .getFirstResult();
    }
    
    public int update( String nameToBeChanged, String newName, Integer newAge ) {
        return JpaCriteriaHelper
                .update(em, MyEntity.class)
                .set("name", newName)
                .set("age", newAge)
                .where("name", nameToBeChanged)
                .execute();
    }
    
}
