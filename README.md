# atlan-migration

Use the atlan go sdk.

Retrieve users from Atlan - API - https://developer.atlan.com/snippets/users-groups/read/#retrieve-all-users
```go
    users, atlanErr := ctx.UserClient.GetAll(20, 0, "") // 
    for _, user := range users { // 
    // Do something with the user...
}
```
One time migration.
This to move existing users on atlan to dataverse-atlan-users.
Add all atlan users to Atlan Data Users group.
