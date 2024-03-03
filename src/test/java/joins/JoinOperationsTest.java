package joins;


import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

public class JoinOperationsTest {

    @Test
    public void testInnerJoin() {
        List<Person> persons = Arrays.asList(
                new Person(1, "Alice", 1),
                new Person(2, "Bob", 2),
                new Person(3, "Charlie", 3)
        );

        List<Department> departments = Arrays.asList(
                new Department(1, "HR"),
                new Department(2, "Engineering"),
                new Department(3, "Marketing")
        );

        List<Pair<Person, Department>> results = Extensions.innerJoin(
                persons, departments,
                Person::getDepartmentId, Department::getId,
                Pair::new
        );

        assertEquals(3, results.size());
        assertTrue(results.contains(new Pair<>(persons.get(0), departments.get(0))));
    }

    @Test
    public void testLeftJoinWithNoMatchingRight() {
        List<Person> persons = Arrays.asList(
                new Person(1, "Alice", 1),
                new Person(2, "Bob", 4) // No matching department
        );

        List<Department> departments = Arrays.asList(
                new Department(1, "HR")
        );

        List<Pair<Person, Department>> results = Extensions.leftJoin(
                persons, departments,
                Person::getDepartmentId, Department::getId,
                Pair::new, null
        );

        assertEquals(2, results.size());
        assertNotNull(results.get(0).getRight()); // Should have a department
        assertNull(results.get(1).getRight()); // Should not have a department
    }
}

