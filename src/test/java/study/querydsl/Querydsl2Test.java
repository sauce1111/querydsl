package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
class Querydsl2Test {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    void before() {
        queryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    void simpleProjection() {
        List<String> results = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String result : results) {
            System.out.println("result = " + result);
        }
    }

    @Test
    void tupleProjection() {
        List<Tuple> results = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple result : results) {
            String username = result.get(member.username);
            Integer age = result.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    void findDtoByJPQL() {
        List<MemberDto> results = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m ", MemberDto.class)
                .getResultList();

        for (MemberDto result : results) {
            System.out.println("result = " + result);
        }
    }

    @Test
    void findDtoBySetter() {
        List<MemberDto> memberDtos = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : memberDtos) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findDtoByField() {
        List<MemberDto> memberDtos = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : memberDtos) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findDtoByConstructor() {
        List<UserDto> memberDtos = queryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (UserDto memberDto : memberDtos) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findUserDto() {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> userDtos = queryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : userDtos) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    void findDtoByQueryProjection() {
        List<MemberDto> memberDtos = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : memberDtos) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void dynamicQueryBooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> results = searchMember1(usernameParam, ageParam);
        assertThat(results.size()).isEqualTo(2);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

    return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    void dynamicQueryWhereParam() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> results = searchMember2(usernameParam, ageParam);
        assertThat(results.size()).isEqualTo(2);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(member)
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private BooleanExpression usernameEq(String usernameCond) {
        if (usernameCond == null) {
            return null;
        }
        return member.username.eq(usernameCond);
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    /**
     * 조합을 통해 의미 있는 이름의 조건으로 만들어서 재사용 가능
     */
    private Predicate allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    @Test
//    @Commit
    void bulkUpdate() {
        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();
        // update 이후에는 DB 의 값과 영속성컨텍스트의 값이 다르다.
        // -> 영속성 컨텍스트를 비워주자.

        em.flush();
        em.clear();

        List<Member> results = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member result : results) {
            System.out.println("username = " + result.getUsername());
        }
    }

    @Test
    void bulkAdd() {
        queryFactory
                .update(member)
//                .set(member.age, member.age.add(1))
                .set(member.age, member.age.multiply(2))
                .execute();
    }

    @Test
    void bulkDelete() {
        queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    void sqlFunction() {
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0},{1},{2})",
                        member.username, "member", "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void sqlFunction2() {
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate(
//                                "function('lower), {0}", member.username)
//                ))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }




}