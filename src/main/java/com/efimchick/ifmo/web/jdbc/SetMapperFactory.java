package com.efimchick.ifmo.web.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
//        throw new UnsupportedOperationException();
        return new SetMapper<Set<Employee>>() {
            @Override
            public Set<Employee> mapSet(ResultSet rs) {
                Set<Employee> employees = new HashSet<Employee>();
                try {
                    while (rs.next()) {
                        employees.add(getEmployee(rs));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
                return employees;
            }
        };
    }

    private Employee getEmployee(ResultSet rs) {
        try {
            return new Employee(new BigInteger(rs.getString("id")),
                                new FullName(
                                        rs.getString("firstname"),
                                        rs.getString("lastname"),
                                        rs.getString("middlename")
                                ),
                                Position.valueOf(rs.getString("position")),
                                LocalDate.parse(rs.getString("hiredate")),
                                new BigDecimal(rs.getString("salary")),
                                getEmployeesManager(rs)
                                );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Employee getEmployeesManager(ResultSet rs) throws SQLException {
        int rowToReturn = rs.getRow();
        BigInteger managerId = BigInteger.valueOf(rs.getInt("manager"));
        if (managerId.equals(BigInteger.valueOf(0))) {
            return null;
        } else {
            Employee manager = null;
            rs.beforeFirst();
            while (rs.next()) {
                if (BigInteger.valueOf(rs.getInt("id")).equals(managerId)) {
                    manager = getEmployee(rs);
                }
            }
            rs.absolute(rowToReturn);
            return manager;
        }
    }
}
