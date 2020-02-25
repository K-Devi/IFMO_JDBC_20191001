package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class ServiceFactory {

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                return getResultsByPage("select * from employee order by hireDate", paging);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                return getResultsByPage("select * from employee order by lastName", paging);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                return getResultsByPage("select * from employee order by salary", paging);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                return getResultsByPage("select * from employee order by department, lastName", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                return getResultsByPage("select * from employee where department=" + department.getId() + "order by hireDate", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                return getResultsByPage("select * from employee where department=" + department.getId() + "order by salary", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                return getResultsByPage("select * from employee where department=" + department.getId() + "order by lastName", paging);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                return getResultsByPage("select * from employee where manager=" + manager.getId() + "order by lastName", paging);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                return getResultsByPage("select * from employee where manager=" + manager.getId() + "order by hireDate", paging);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                return getResultsByPage("select * from employee where manager=" + manager.getId() + "order by salary", paging);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                return getSortedEmployees(true, true, "select * from employee where id = " + employee.getId()).get(0);
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                return getSortedEmployees(false, true, "select * from employee where department=" + department.getId() + " order by salary desc").get(salaryRank-1);
            }
        };
    }

    private ResultSet execQueryAndGetRS(String query) {
        try {
            Statement statement = ConnectionSource.instance().createConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            return statement.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Employee getEmployee(ResultSet rs, boolean implementManager, boolean withChain) throws SQLException {
        Employee manager = null;
        Department dep = null;
        if ((withChain || implementManager) && rs.getObject("manager") != null) {
            BigInteger managerId = new BigInteger(rs.getString("manager"));
            manager = getSortedEmployees(withChain, false, "select * from employee where id=" + managerId).get(0);
        }
        if (rs.getObject("department") != null) {
            dep = getDepById(BigInteger.valueOf(rs.getInt("department")));
        }
        return new Employee(new BigInteger(rs.getString("id")),
                            new FullName(
                                    rs.getString("firstName"),
                                    rs.getString("lastName"),
                                    rs.getString("middleName")
                            ),
                            Position.valueOf(rs.getString("position")),
                            LocalDate.parse(rs.getString("hireDate")),
                            rs.getBigDecimal("salary"),
                            manager,
                            dep
                            );
    }


    private Department getDep(ResultSet rs) throws SQLException {
        return new Department(new BigInteger(rs.getString("id")),
                              rs.getString("name"),
                              rs.getString("location")
                              );
    }

    private Department getDepById(BigInteger id) {
        String query = "select * from department where id=" + id;
        try {
            ResultSet rs = execQueryAndGetRS(query);
            Objects.requireNonNull(rs).next();
            return getDep(rs);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Employee> getSortedEmployees(boolean withChain, boolean implementManager, String query) {
        try {
            List<Employee> list = new LinkedList<Employee>();
            ResultSet rs = execQueryAndGetRS(query);
            while (rs.next()) {
                Employee employee = getEmployee(rs, implementManager, withChain);
                list.add(employee);
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Employee> getResultsByPage(String query, Paging paging) {
        String query1 = query + " offset " + (paging.page-1)*paging.itemPerPage + " limit " + paging.itemPerPage;
        return getSortedEmployees(false, true, query1);
    }
}
