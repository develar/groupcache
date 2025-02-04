package groupcache_test

import (
    "context"
    "fmt"
    "github.com/develar/groupcache/examplepb"
    "log"
    "time"

    "github.com/develar/groupcache"
)

func ExampleUsage() {
	/*
		// Keep track of peers in our cluster and add our instance to the pool `http://localhost:8080`
		pool := groupcache.NewHTTPPoolOpts("http://localhost:8080", &groupcache.HTTPPoolOptions{})

		// Add more peers to the cluster
		//pool.Set("http://peer1:8080", "http://peer2:8080")

		server := http.Server{
			Addr:    "localhost:8080",
			Handler: pool,
		}

		// Start a HTTP server to listen for peer requests from the groupcache
		go func() {
			log.Printf("Serving....\n")
			if err := server.ListenAndServe(); err != nil {
				log.Fatal(err)
			}
		}()
		defer server.Shutdown(context.Background())
	*/

	// Create a new group cache with a max cache size of 3 MB
	group := groupcache.NewGroup("users", 30_00_000, groupcache.GetterFunc(
		func(ctx context.Context, id string) (groupcache.Value, time.Time, error) {
			// In a real scenario we might fetch the value from a database.
			/*if user, err := fetchUserFromMongo(ctx, id); err != nil {
				return err
			}*/

			user := examplepb.User{
				Id:      "12345",
				Name:    "John Doe",
				Age:     40,
				IsSuper: true,
			}

			// Set the user in the groupcache to expire after 5 minutes
			return &user, time.Now().Add(time.Minute*5), nil
		}), func() groupcache.Value {
        return &examplepb.User{}
    })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

    v, err := group.Get(ctx, "12345")
	if err != nil {
		log.Fatal(err)
	}

    user := v.(*examplepb.User)

	fmt.Printf("-- User --\n")
	fmt.Printf("Id: %s\n", user.Id)
	fmt.Printf("Name: %s\n", user.Name)
	fmt.Printf("Age: %d\n", user.Age)
	fmt.Printf("IsSuper: %t\n", user.IsSuper)

	/*
		// Remove the key from the groupcache
		if err := group.Remove(ctx, "12345"); err != nil {
			fmt.Printf("Remove Err: %s\n", err)
			log.Fatal(err)
		}
	*/

	// Output: -- User --
	// Id: 12345
	// Name: John Doe
	// Age: 40
	// IsSuper: true
}
