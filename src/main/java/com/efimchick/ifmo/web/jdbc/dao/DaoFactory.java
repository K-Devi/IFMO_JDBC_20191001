package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DaoFactory {
    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                String query = "select * from employee where department=" + department.getId();
                return getEmpList(execQueryAndGetRS(query));
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                String query = "select * from employee where manager=" + employee.getId();
                return getEmpList(execQueryAndGetRS(query));
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                String query = "select * from employee where id=" + Id;
                try {
                    ResultSet rs = execQueryAndGetRS(query);
                    assert rs != null;
                    if (rs.next())
                        return Optional.of(Objects.requireNonNull(getEmployee(rs)));
                    else
                        return Optional.empty();
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                String query = "select * from employee";
                return getEmpList(execQueryAndGetRS(query));
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    String query = "insert into employee values (?,?,?,?,?,?,?,?,?)";
                    PreparedStatement prepStatement = ConnectionSource.instance().createConnection().prepareStatement(query);
                    prepStatement.setInt(1, employee.getId().intValue());
                    prepStatement.setString(2, employee.getFullName().getFirstName());
                    prepStatement.setString(3, employee.getFullName().getLastName());
                    prepStatement.setString(4, employee.getFullName().getMiddleName());
                    prepStatement.setString(5, employee.getPosition().toString());
                    prepStatement.setInt(6, employee.getManagerId().intValue());
                    prepStatement.setDate(7, Date.valueOf(employee.getHired()));
                    prepStatement.setDouble(8, employee.getSalary().doubleValue());
                    prepStatement.setInt(9, employee.getDepartmentId().intValue());
                    int res = prepStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                String query = "delete from employee where id=" + employee.getId().toString();
                try {
                    deleteByQuery(query);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
//        throw new UnsupportedOperationException();
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                String query = "select * from department where id=" + Id.toString();
                try {
                    ResultSet rs = execQueryAndGetRS(query);
                    if (rs.next()) {
                        return Optional.ofNullable(getDep(rs));
                    }
                    else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                List<Department> departments = new ArrayList<Department>();
                String query = "select * from department";
                try {
                    ResultSet rs = execQueryAndGetRS(query);
                    while (rs.next()) {
                        departments.add(getDep(rs));
                    }
                    return departments;
                } catch (SQLException e) {
                    e.getErrorCode();
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try {
                    PreparedStatement prepStatement;
                    String query;
                    if (getById(department.getId()).equals(Optional.empty())) {
                        query = "insert into department values (?,?,?)";
                        prepStatement = getPreparedStatement(query);
                        prepStatement.setInt(1, department.getId().intValue());
                        prepStatement.setString(3, department.getLocation());
                    } else {
                        query = "update department set location=?, name=? where id=?";
                        prepStatement = getPreparedStatement(query);
                        prepStatement.setString(1, department.getLocation());
                        prepStatement.setInt(3, department.getId().intValue());
                    }
                    prepStatement.setString(2, department.getName());
                    int changedRows = prepStatement.executeUpdate();
                    System.out.println(changedRows);
                    return department;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public void delete(Department department) {
                String query = "delete from department where id=" + department.getId().toString();
                try {
                    deleteByQuery(query);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private ResultSet execQueryAndGetRS(String query) {
        try {
            return ConnectionSource.instance().createConnection().createStatement().executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<Employee> getEmpList(ResultSet rs) {
        List<Employee> employees = new ArrayList<Employee>();
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

    private Employee getEmployee(ResultSet rs) {
        try {
            BigInteger managerId = BigInteger.ZERO;

            if (rs.getString("manager") != null) {
                managerId = new BigInteger(rs.getString("manager"));
            }

            BigInteger depId = BigInteger.ZERO;

            if (rs.getString("department") != null) {
                depId = new BigInteger(rs.getString("department"));
            }

            return new Employee(new BigInteger(rs.getString("id")),
                                new FullName(
                                        rs.getString("firstname"),
                                        rs.getString("lastname"),
                                        rs.getString("middlename")
                                ),
                                Position.valueOf(rs.getString("position")),
                                LocalDate.parse(rs.getString("hiredate")),
                                new BigDecimal(rs.getString("salary")),
                                managerId,
                                depId
                                );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Department getDep(ResultSet rs) {
        try {
            return new Department(new BigInteger(rs.getString("id")),
                                  rs.getString("name"),
                                  rs.getString("location")
                                 );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    private PreparedStatement getPreparedStatement(String query) throws SQLException {
       return ConnectionSource.instance().createConnection().prepareStatement(query);
    }

    private void deleteByQuery(String query) throws SQLException {
        PreparedStatement prepStatement = getPreparedStatement(query);
        prepStatement.execute();
    }
}
