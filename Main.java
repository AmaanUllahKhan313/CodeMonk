package co.cantina.jooq.example;/******/

import static co.cantina.jooq.example.Sequences.AUTHOR_SEQ;
import static co.cantina.jooq.example.Sequences.BOOK_SEQ;
import static co.cantina.jooq.example.Tables.AUTHOR;
import static co.cantina.jooq.example.Tables.BOOK;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.ZonedDateTime;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class Main {
    
    private static void demonstrateInsert(final DSLContext dsl) {
        
        Long evansId = dsl.nextval(AUTHOR_SEQ);
        Long vernonId = dsl.nextval(AUTHOR_SEQ);
        
        dsl.batch(
            dsl.insertInto(AUTHOR)
               .set(AUTHOR.AUTHOR_ID, evansId)
               .set(AUTHOR.FULL_NAME, "Eric Evans"),
                
            dsl.insertInto(AUTHOR)
               .set(AUTHOR.AUTHOR_ID, vernonId)
               .set(AUTHOR.FULL_NAME, "Vaughn Vernon"),
                
            dsl.insertInto(BOOK)
               .set(BOOK.BOOK_ID, dsl.nextval(BOOK_SEQ))
               .set(BOOK.AUTHOR_ID, evansId)
               .set(BOOK.TITLE, "Domain Driven Design"),
                
            dsl.insertInto(BOOK)
               .set(BOOK.BOOK_ID, dsl.nextval(BOOK_SEQ))
               .set(BOOK.AUTHOR_ID, vernonId)
               .set(BOOK.TITLE, "Implementing Domain Driven Design")
        ).execute();
    }
    
    private static void demonstrateActiveRecord(final DSLContext dsl) {
        dsl.selectFrom(AUTHOR)
           .fetch()
           .stream()
           .forEach(a -> {
               System.out.println("======From Fetch Into =======\n" 
                                  + a.getFullName()); 
           });
    }
    
    private static void demonstrateDataConversion(final DSLContext dsl) {
        
        ZonedDateTime date = dsl
            .select(BOOK.DATE_ADDED)
            .from(BOOK)
            .where(BOOK.TITLE.like("Domain%"))
            .fetchOne(BOOK.DATE_ADDED);
        
                
        System.out.printf(
            "=============== Data Conversion ================\n"
            + "Date Added: %s\n",
            date
        );  
    }
    
    private static void demonstrateStreams(final DSLContext dsl) {
       
        dsl.select(
            BOOK.TITLE, 
            AUTHOR.FULL_NAME
        )
       .from(BOOK)
       .join(AUTHOR)
       .using(BOOK.AUTHOR_ID)
       .fetch()
       .stream()
       .map(r -> { 
            return new Book(
                r.getValue(BOOK.TITLE), 
                new Author(r.getValue(AUTHOR.FULL_NAME))
            );
       })
       .forEach(b -> {
            System.out.printf(
                  "=============== From Stream ================\n"
                + "BOOK.TITLE: %s\n"
                + "AUTHOR.FULL_NAME: %s\n",
                b.getTitle(), b.getAuthor().getFullName()
            );  
       });
    }
    
    private static void demonstrateDelete(final DSLContext dsl) {
        int numBooksDeleted = dsl.delete(BOOK).execute();
        int numAuthorsDeleted = dsl.delete(AUTHOR).execute();
        
        System.out.printf("============ Deletes =============\n" 
                          + "Deleted %d books and %d authors\n", 
                          numBooksDeleted, numAuthorsDeleted);
    }
    
    public static void main(final String[] args) {
        
        String userName = System.getProperty("user.name");
        String password = "";
        String url = "jdbc:postgresql://localhost:5432/" + userName;

        try (Connection conn = DriverManager.getConnection(url, userName, password)) {

            DSLContext dsl = DSL.using(conn, SQLDialect.POSTGRES);
            
            demonstrateInsert(dsl);
            demonstrateActiveRecord(dsl);
            demonstrateStreams(dsl);
            demonstrateDataConversion(dsl);
            demonstrateDelete(dsl);
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
